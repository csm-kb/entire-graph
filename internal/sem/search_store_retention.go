package sem

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Search store retention bounds the sqlite sidecars. A store is 10-30x the size
// of the gz entry it mirrors (68 MB against 3.5 MB on this repository, 196 MB
// against 7.4 MB on entire-brain), the cache key changes with every new tree,
// and nothing else ever removes one, so a consumer that builds a store per
// commit grows the cache without bound. Each publish therefore trims the
// sidecars of its own repository and scope to the newest few. The gz entries
// are left alone: they are small, they stay authoritative, and
// repairSearchStoreSidecarForEntry turns one back into a store in about two
// seconds without reparsing, so evicting a sidecar costs at most that rebuild.
//
// What is kept, per group (the store's repository key plus its checkout scope):
//   - the newest searchStoreKeep stores by modification time, which a publish
//     sets, admitted newest first while their total stays within
//     searchStoreRetainBytes; the single newest store is always admitted;
//   - the key being published, wherever it sorts;
//   - every store built for the publishing checkout's current HEAD tree, in
//     any policy variant, so a writer for an older tree can never evict the
//     store that checkout is reading now.
//
// The group deliberately leaves out the provider version, the profile and the
// ignore policy. Those are in the cache key, so a group that included them
// would strand the stores of every superseded nightly and every edited ignore
// file forever, each group under its own limit. Variants of one repository
// share the limit instead; the HEAD rule keeps each variant's current store.
//
// Readers are never locked out. Graph's own readers and external consumers
// (docs/store-contract.md) open a store by path, read-only and immutable, and
// a published store is never modified, renamed or renamed over: each publish
// writes a new generation under a fresh name in its key's directory
// (search_store_generation.go). A pass therefore only ever removes names it
// listed, whose content cannot have changed since, and needs no lock against
// publishers or other passes. On POSIX a removal unlinks the name while every
// open connection keeps reading the same inode; Windows refuses to delete a
// file another process holds open, and that removal error skips the store
// until a later publish retries it.
//
// Two passes follow each publish. The superseded sweep
// (sweepSupersededSearchStores), at any KEEP, removes every generation of
// every key other than the key's current one and the one just published, and
// temporaries older than searchStoreTempMaxAge. The window pass below, when
// KEEP > 0, trims whole keys: one candidate per key directory, aged and
// identified by its current generation, plus legacy <key>.sqlite files.
const (
	// searchStoreKeepEnv overrides searchStoreKeep. 0 or a negative value
	// turns retention off, which is the behaviour before it existed.
	searchStoreKeepEnv = "ENTIRE_GRAPH_SEARCH_STORE_KEEP"
	// defaultSearchStoreKeep is three: the store for HEAD plus two before it.
	// That covers a checkout that flips between two trees (a branch switch, a
	// rebase and back) and two or three worktrees of one repository indexing at
	// once, and leaves the nearest ancestors for a reader that falls back to
	// one. At the measured sizes it holds 204-588 MB per repository. Unbounded,
	// one store per first-parent commit came to about 1.2 GB a week on
	// entire-graph (17 commits) and 17 GB a week on entire-brain (86).
	defaultSearchStoreKeep = 3
	// searchStoreRetainBytes caps the recency window by size as well as by
	// count. At five times the largest store measured (196 MB) it never binds
	// before the count does on repositories of that size; it starts to bind
	// only where one store passes about 340 MB, at which point keeping older
	// copies costs more disk than rebuilding one from its gz entry costs time.
	searchStoreRetainBytes int64 = 1 << 30
)

// searchStoreTempMaxAge is how old a temporary must be before a sweep removes
// it as left by a crashed build. A build in progress keeps its temporary far
// younger: the Linux kernel's store builds in about four minutes.
const searchStoreTempMaxAge = 24 * time.Hour

// Test seams. beforeSearchStorePublish runs after the store is built and
// before it is renamed into place; an error it returns fails the publish, as
// a failed rename would. afterSearchStorePublish runs right after the rename;
// beforeSearchStoreEvictionRemove runs after an eviction pass has chosen what
// to remove and before it removes anything. They let a test force the
// interleaving of a writer and an evictor, or a failed publish.
// removeSearchStoreSidecar is the removal every pass uses, so a test can
// stand in for a platform that refuses to delete an open file.
var (
	beforeSearchStorePublish        func() error
	afterSearchStorePublish         func()
	beforeSearchStoreEvictionRemove func()
	removeSearchStoreSidecar        = (*os.Root).Remove
)

// resolveSearchStoreKeep reads the override, falling back to the default for
// anything unparseable so a typo cannot silently turn retention off.
func resolveSearchStoreKeep() int {
	raw, ok := os.LookupEnv(searchStoreKeepEnv)
	if !ok {
		return defaultSearchStoreKeep
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return defaultSearchStoreKeep
	}
	if value <= 0 {
		return 0
	}
	return value
}

// searchStoreRetention is one eviction pass's policy.
type searchStoreRetention struct {
	keep     int
	maxBytes int64
	// group is searchStoreRetentionGroup of the store being published. Only
	// stores in this group are considered.
	group string
	// protectNames are candidate names never evicted: the key directory of the
	// store being published.
	protectNames map[string]bool
	// protectTree, when set, is the publishing checkout's current HEAD tree.
	protectTree string
	// remove deletes one store; os.Root.Remove in production.
	remove func(directory *os.Root, name string) error
}

// searchStoreEviction reports what a pass did. Skipped maps a store that was
// chosen for eviction but could not be removed to the removal error.
type searchStoreEviction struct {
	Evicted []string
	Kept    []string
	Skipped map[string]error
}

type searchStoreCandidate struct {
	name    string
	size    int64
	modTime time.Time
	tree    string
	// generations are the generation names a key directory candidate listed,
	// the only names its eviction removes; nil for a legacy <key>.sqlite file.
	generations []string
}

func searchStoreRetentionGroup(repositoryKey, repoScope string) string {
	return repositoryKey + "\x00" + repoScope
}

// searchStoreSidecarName reports whether name is a legacy store, a cache key
// followed by .sqlite, published before stores moved into key directories.
// Such a file is aged out by the window and never read. Temporaries
// (".store-*.json.gz"), gz entries, lock files and derived-entry directories
// never match.
func searchStoreSidecarName(name string) bool {
	key, ok := strings.CutSuffix(name, ".sqlite")
	return ok && validSHA256Hex(key)
}

// newSearchStoreRetention builds the policy for publishing store, or reports
// false when retention is off. It resolves the publishing checkout's HEAD tree,
// which the window protects.
func newSearchStoreRetention(ctx context.Context, store cacheEntry, cache *cachedSearchSnapshot) (searchStoreRetention, bool) {
	keep := resolveSearchStoreKeep()
	if keep <= 0 {
		return searchStoreRetention{}, false
	}
	retention := searchStoreRetention{
		keep:         keep,
		maxBytes:     searchStoreRetainBytes,
		group:        searchStoreRetentionGroup(cache.Snapshot.Header.RepoKey, cache.RepoScope),
		protectNames: map[string]bool{filepath.Base(store.relative): true},
		remove:       removeSearchStoreSidecar,
	}
	if root := cache.Snapshot.Header.RepoRoot; root != "" {
		if _, tree, err := resolveCommittedHEAD(ctx, root); err == nil {
			retention.protectTree = tree
		}
	}
	return retention, true
}

// sweepSupersededSearchStores removes, from every key directory in directory,
// each generation other than the key's current one (orderSearchStoreGenerations
// against the key's gz entry) and the one this publish just wrote
// (publishedName in publishedKeyDir), and each temporary older than
// searchStoreTempMaxAge. It runs after every publish whatever the recency
// window says, and takes no lock: a generation's content never changes, so a
// removal can only ever delete what was listed. A key whose gz entry cannot be
// stamped is skipped, since its current generation cannot be told apart. A
// removal that fails (Windows: a reader holds the file) is skipped; the next
// publish retries it.
func sweepSupersededSearchStores(ctx context.Context, directory *os.Root, directoryPath, publishedKeyDir, publishedName string, remove func(*os.Root, string) error) {
	if ctx.Err() != nil {
		return
	}
	names, err := listCacheDirectoryNames(directory)
	if err != nil {
		return
	}
	now := time.Now()
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		if searchStoreKeyDirName(name) {
			sweepSearchStoreKeyDir(ctx, directory, directoryPath, name, publishedKeyDir, publishedName, remove, now)
		}
	}
}

func sweepSearchStoreKeyDir(ctx context.Context, directory *os.Root, directoryPath, name, publishedKeyDir, publishedName string, remove func(*os.Root, string) error, now time.Time) {
	keyDirPath := filepath.Join(directoryPath, name)
	entries, err := os.ReadDir(keyDirPath)
	if err != nil {
		return
	}
	generationCount := 0
	var temporaries []string
	for _, entry := range entries {
		switch {
		case searchStoreGenerationName(entry.Name()):
			generationCount++
		case searchStoreTempName(entry.Name()):
			temporaries = append(temporaries, entry.Name())
		}
	}
	if generationCount < 2 && len(temporaries) == 0 {
		return
	}
	keyDir, err := openCacheComponent(directory, name, (*os.Root).OpenRoot)
	if err != nil {
		return
	}
	defer keyDir.Close()
	// List, then stamp, as the resolver does. A generation listed here was
	// published from an entry written before the stamp is taken, so a publish
	// landing in between can never be ordered against a stale stamp and
	// removed as superseded.
	generations, err := listSearchStoreGenerations(ctx, keyDirPath)
	if err != nil {
		return
	}
	key := strings.TrimSuffix(name, searchStoreKeyDirSuffix)
	entryStamp, err := currentSearchEntryStamp(cacheEntry{root: directoryPath, relative: key + ".json.gz"})
	if err != nil {
		return
	}
	orderSearchStoreGenerations(generations, &entryStamp)
	for index, generation := range generations {
		if index == 0 || (name == publishedKeyDir && generation.name == publishedName) {
			continue
		}
		_ = remove(keyDir, generation.name)
	}
	for _, temporary := range temporaries {
		if info, err := keyDir.Lstat(temporary); err == nil && now.Sub(info.ModTime()) > searchStoreTempMaxAge {
			_ = remove(keyDir, temporary)
		}
	}
}

// evictSearchStoreSidecars removes the stores in the policy's group that fall
// outside it. directoryPath is the filesystem path of directory, which sqlite
// needs to read each store's identity. A store whose identity cannot be read
// is never removed: retention only deletes what it can attribute to the group
// being trimmed. Evicting a key removes only the generations this pass listed;
// the key directory stays. The result records generation names, legacy file
// names, and the name of a key directory that could not be opened.
func evictSearchStoreSidecars(ctx context.Context, directory *os.Root, directoryPath string, retention searchStoreRetention) (searchStoreEviction, error) {
	candidates, err := listSearchStoreGroup(ctx, directory, directoryPath, retention.group)
	if err != nil {
		return searchStoreEviction{}, err
	}
	keep, evict := planSearchStoreEviction(candidates, retention)
	result := searchStoreEviction{Kept: keep}
	if len(evict) == 0 {
		return result, nil
	}
	if beforeSearchStoreEvictionRemove != nil {
		beforeSearchStoreEvictionRemove()
	}
	listed := make(map[string]searchStoreCandidate, len(candidates))
	for _, candidate := range candidates {
		listed[candidate.name] = candidate
	}
	for _, name := range evict {
		candidate := listed[name]
		if candidate.generations == nil {
			result.record(name, retention.remove(directory, name))
			continue
		}
		keyDir, err := openCacheComponent(directory, name, (*os.Root).OpenRoot)
		if err != nil {
			result.record(name, err)
			continue
		}
		for _, generation := range candidate.generations {
			result.record(generation, retention.remove(keyDir, generation))
		}
		_ = keyDir.Close()
	}
	return result, nil
}

// record files one removal's outcome under name.
func (result *searchStoreEviction) record(name string, err error) {
	switch {
	case err == nil:
		result.Evicted = append(result.Evicted, name)
	case errors.Is(err, fs.ErrNotExist):
		// Already gone: removed by hand, or by another pass.
	default:
		// Windows refuses to delete a file another process holds open, as
		// a reader of this store does. Leave it; a later publish retries.
		if result.Skipped == nil {
			result.Skipped = make(map[string]error)
		}
		result.Skipped[name] = err
	}
}

// listCacheDirectoryNames returns the names in directory, unsorted.
func listCacheDirectoryNames(directory *os.Root) ([]string, error) {
	handle, err := directory.Open(".")
	if err != nil {
		return nil, err
	}
	names, err := handle.Readdirnames(-1)
	_ = handle.Close()
	return names, err
}

// listSearchStoreGroup returns the stores in directory that belong to group:
// one candidate per key directory, plus each legacy <key>.sqlite file.
func listSearchStoreGroup(ctx context.Context, directory *os.Root, directoryPath, group string) ([]searchStoreCandidate, error) {
	names, err := listCacheDirectoryNames(directory)
	if err != nil {
		return nil, err
	}
	var candidates []searchStoreCandidate
	for _, name := range names {
		switch {
		case searchStoreKeyDirName(name):
			if candidate, ok := searchStoreKeyCandidate(ctx, directory, directoryPath, name, group); ok {
				candidates = append(candidates, candidate)
			}
		case searchStoreSidecarName(name):
			info, err := directory.Lstat(name)
			if err != nil || !info.Mode().IsRegular() || pathMayRedirect(info) {
				continue
			}
			storeGroup, tree, ok := readSearchStoreRetentionIdentity(ctx, filepath.Join(directoryPath, name))
			if !ok || storeGroup != group {
				continue
			}
			candidates = append(candidates, searchStoreCandidate{name: name, size: info.Size(), modTime: info.ModTime(), tree: tree})
		}
	}
	return candidates, nil
}

// searchStoreKeyCandidate is a key directory's retention candidate. Its
// identity (group and tree) and its age come from the key's current
// generation (orderSearchStoreGenerations against the key's gz entry); its
// size is every generation's, since evicting the key removes them all.
func searchStoreKeyCandidate(ctx context.Context, directory *os.Root, directoryPath, name, group string) (searchStoreCandidate, bool) {
	info, err := directory.Lstat(name)
	if err != nil || !info.IsDir() || pathMayRedirect(info) {
		return searchStoreCandidate{}, false
	}
	generations, err := listSearchStoreGenerations(ctx, filepath.Join(directoryPath, name))
	if err != nil || len(generations) == 0 {
		return searchStoreCandidate{}, false
	}
	key := strings.TrimSuffix(name, searchStoreKeyDirSuffix)
	var entryStamp *searchStoreEntryStamp
	if stamp, err := currentSearchEntryStamp(cacheEntry{root: directoryPath, relative: key + ".json.gz"}); err == nil {
		entryStamp = &stamp
	}
	orderSearchStoreGenerations(generations, entryStamp)
	storeGroup, tree, ok := readSearchStoreRetentionIdentity(ctx, generations[0].path)
	if !ok || storeGroup != group {
		return searchStoreCandidate{}, false
	}
	candidate := searchStoreCandidate{name: name, modTime: generations[0].modTime, tree: tree}
	for _, generation := range generations {
		candidate.size += generation.size
		candidate.generations = append(candidate.generations, generation.name)
	}
	return candidate, true
}

// readSearchStoreRetentionIdentity reads the group and tree a store was built
// for from its meta table. Every store version carries both envelope and
// header, so stores of an older version are grouped (and aged out) like
// current ones.
func readSearchStoreRetentionIdentity(ctx context.Context, path string) (string, string, bool) {
	db, err := sql.Open("sqlite", "file:"+sqliteURLPath(path)+"?immutable=1&mode=ro")
	if err != nil {
		return "", "", false
	}
	defer db.Close()
	var envelope searchStoreMeta
	if err := readSearchStoreMetaKey(ctx, db, "envelope", &envelope); err != nil {
		return "", "", false
	}
	var header struct {
		RepoKey string `json:"repo_key"`
	}
	if err := readSearchStoreMetaKey(ctx, db, "header", &header); err != nil {
		return "", "", false
	}
	return searchStoreRetentionGroup(header.RepoKey, envelope.RepoScope), envelope.Tree, true
}

// planSearchStoreEviction splits one group's stores into the names kept and
// the names to evict. Stores are admitted to the recency window newest first
// (ties broken by name, so every evictor orders them alike) until the count or
// the byte budget is exhausted; the first store refused closes the window, so
// what survives is always a prefix of the newest. Protected stores are kept
// wherever they sort.
//
// The rule is what makes concurrent passes safe without a lock: a pass
// evicts a store only when at least keep stores newer than it exist, or when
// newer stores already fill the byte budget, and a store with that many newer
// ones is refused by every later pass too. No interleaving can therefore evict
// one of the newest stores that fit.
func planSearchStoreEviction(candidates []searchStoreCandidate, retention searchStoreRetention) (keep, evict []string) {
	sorted := append([]searchStoreCandidate(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].modTime.Equal(sorted[j].modTime) {
			return sorted[i].modTime.After(sorted[j].modTime)
		}
		return sorted[i].name < sorted[j].name
	})
	admitted, admittedBytes := 0, int64(0)
	windowOpen := true
	for index, candidate := range sorted {
		inWindow := false
		if windowOpen {
			if index == 0 || (admitted < retention.keep && admittedBytes+candidate.size <= retention.maxBytes) {
				inWindow = true
				admitted++
				admittedBytes += candidate.size
			} else {
				windowOpen = false
			}
		}
		protected := retention.protectNames[candidate.name] ||
			(retention.protectTree != "" && candidate.tree == retention.protectTree)
		if inWindow || protected {
			keep = append(keep, candidate.name)
		} else {
			evict = append(evict, candidate.name)
		}
	}
	return keep, evict
}
