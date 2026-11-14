package sem

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const retentionTestVersion = "retention-test"

// retentionTestCache builds one small real snapshot and returns a factory for
// caches that differ only in the identity retention groups and protects by.
func retentionTestCache(t *testing.T) func(repoKey, scope, tree string) *cachedSearchSnapshot {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "widget.go"), []byte("package sample\n\nfunc Render() string { return \"x\" }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := BuildProviderSnapshotWithOptions(t.Context(), dir, "store-test", ProviderSnapshotOptions{Worktree: true})
	if err != nil {
		t.Fatal(err)
	}
	base := newCachedSearchSnapshot("store-test", "commit", "tree", ProviderSnapshotOptions{}, snapshot, t.Context())
	return func(repoKey, scope, tree string) *cachedSearchSnapshot {
		cache := base
		cache.Tree = tree
		cache.RepoScope = scope
		cache.Snapshot.Header.RepoKey = repoKey
		return &cache
	}
}

func retentionTestKey(index int) string {
	return fmt.Sprintf("%064x", index)
}

// writeAndPublishSearchStore writes key's gz entry under version and publishes
// its store the way production does, with the entry's real digest.
func writeAndPublishSearchStore(t *testing.T, cacheDir, version, key string, cache *cachedSearchSnapshot) {
	t.Helper()
	entry, err := newCacheEntry(cacheDir, "search", version, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSearchSnapshot(entry, *cache); err != nil {
		t.Fatal(err)
	}
	digest, err := entry.digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSearchStoreSidecar(t.Context(), entry, digest.String(), cache); err != nil {
		t.Fatal(err)
	}
}

// publishSearchStoreAt is writeAndPublishSearchStore, returning the store file
// the publish added under cacheDir. It must be the only publish in flight.
func publishSearchStoreAt(t *testing.T, cacheDir, version, key string, cache *cachedSearchSnapshot) string {
	t.Helper()
	before := map[string]bool{}
	for _, path := range searchStoreFilesForTest(t, cacheDir) {
		before[path] = true
	}
	writeAndPublishSearchStore(t, cacheDir, version, key, cache)
	var added []string
	for _, path := range searchStoreFilesForTest(t, cacheDir) {
		if !before[path] {
			added = append(added, path)
		}
	}
	if len(added) != 1 {
		t.Fatalf("publishing %s added %v, want exactly one store", key, added)
	}
	return added[0]
}

// publishRetentionTestStore publishes under the retention test version and
// returns the published store's path.
func publishRetentionTestStore(t *testing.T, cacheDir, key string, cache *cachedSearchSnapshot) string {
	t.Helper()
	return publishSearchStoreAt(t, cacheDir, retentionTestVersion, key, cache)
}

func ageRetentionTestStore(t *testing.T, path string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

// retentionTestStores lists every store file under cacheDir, sorted.
func retentionTestStores(t *testing.T, cacheDir string) []string {
	t.Helper()
	return searchStoreFilesForTest(t, cacheDir)
}

func openRetentionTestDirectory(t *testing.T, cacheDir string) (*os.Root, string) {
	t.Helper()
	entry, err := newCacheEntry(cacheDir, "search", retentionTestVersion, retentionTestKey(0))
	if err != nil {
		t.Fatal(err)
	}
	directory, err := openCacheEntryDirectory(entry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directory.Close() })
	return directory, filepath.Join(cacheDir, "search", retentionTestVersion)
}

// Writing K+1 stores into one repository/scope group leaves the newest K. A
// store of another group is never touched, however old, and every gz entry
// survives: retention trims sidecars only.
func TestSearchStoreRetentionKeepsNewestKPerGroup(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "2")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	base := time.Now().Add(-time.Hour)

	other := publishRetentionTestStore(t, cacheDir, retentionTestKey(100), newCache("gh/acme/other", "prefix:", "tree-o"))
	ageRetentionTestStore(t, other, base)
	var published []string
	for index := 1; index <= 2; index++ {
		path := publishRetentionTestStore(t, cacheDir, retentionTestKey(index), newCache("gh/acme/widget", "prefix:", fmt.Sprintf("tree-%d", index)))
		ageRetentionTestStore(t, path, base.Add(time.Duration(index)*time.Minute))
		published = append(published, path)
	}
	p1, p2 := published[0], published[1]
	if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{other, p1, p2}); !equalStrings(got, want) {
		t.Fatalf("premise: K stores must all survive, got %v want %v", got, want)
	}

	p3 := publishRetentionTestStore(t, cacheDir, retentionTestKey(3), newCache("gh/acme/widget", "prefix:", "tree-3"))
	if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{other, p2, p3}); !equalStrings(got, want) {
		t.Fatalf("after K+1 stores: got %v, want the newest two of the group plus the other group's store %v", got, want)
	}
	for _, index := range []int{1, 2, 3, 100} {
		if _, err := os.Stat(filepath.Join(cacheDir, "search", retentionTestVersion, retentionTestKey(index)+".json.gz")); err != nil {
			t.Fatalf("gz entry %d must never be evicted: %v", index, err)
		}
	}
}

// The store being published is kept even when a newer store already fills the
// window (another writer's, or one with a skewed clock), and every store built
// for the publishing checkout's current HEAD tree survives a publish for
// another tree.
func TestSearchStoreRetentionNeverEvictsPublishedOrHeadStore(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "1")
	newCache := retentionTestCache(t)

	t.Run("published", func(t *testing.T) {
		cacheDir := t.TempDir()
		newer := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), newCache("gh/acme/widget", "prefix:", "tree-1"))
		ageRetentionTestStore(t, newer, time.Now().Add(time.Hour))
		p2 := publishRetentionTestStore(t, cacheDir, retentionTestKey(2), newCache("gh/acme/widget", "prefix:", "tree-2"))
		if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{newer, p2}); !equalStrings(got, want) {
			t.Fatalf("the newest store and the one just published must both survive: got %v want %v", got, want)
		}
	})

	t.Run("head", func(t *testing.T) {
		repo := t.TempDir()
		initDiffPolicyRepo(t, repo)
		if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		git(t, repo, "add", "main.go")
		git(t, repo, "commit", "-q", "-m", "seed")
		headTree := gitInput(t, repo, "", "rev-parse", "HEAD^{tree}")
		inRepo := func(tree string) *cachedSearchSnapshot {
			cache := newCache("gh/acme/widget", "prefix:", tree)
			cache.Snapshot.Header.RepoRoot = repo
			return cache
		}
		cacheDir := t.TempDir()
		base := time.Now().Add(-time.Hour)
		head := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), inRepo(headTree))
		ageRetentionTestStore(t, head, base)
		older := publishRetentionTestStore(t, cacheDir, retentionTestKey(2), inRepo("tree-older"))
		ageRetentionTestStore(t, older, base.Add(time.Minute))
		p3 := publishRetentionTestStore(t, cacheDir, retentionTestKey(3), inRepo("tree-elsewhere"))
		if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{head, p3}); !equalStrings(got, want) {
			t.Fatalf("HEAD's store must survive a publish for another tree: got %v want %v", got, want)
		}
	})
}

// The recency window is a prefix of the newest stores bounded by count and by
// bytes, and the single newest store is admitted even when it alone exceeds
// the byte budget.
func TestSearchStoreRetentionByteBudget(t *testing.T) {
	now := time.Now()
	candidate := func(name string, age time.Duration, size int64) searchStoreCandidate {
		return searchStoreCandidate{name: name, size: size, modTime: now.Add(-age)}
	}
	retention := searchStoreRetention{keep: 3, maxBytes: 1000}

	keep, evict := planSearchStoreEviction([]searchStoreCandidate{
		candidate("c.sqlite", 3*time.Minute, 100),
		candidate("a.sqlite", time.Minute, 600),
		candidate("b.sqlite", 2*time.Minute, 500),
	}, retention)
	if !equalStrings(keep, []string{"a.sqlite"}) || !equalStrings(evict, []string{"b.sqlite", "c.sqlite"}) {
		t.Fatalf("budget: kept %v evicted %v; want the window closed at the first store over budget", keep, evict)
	}

	keep, evict = planSearchStoreEviction([]searchStoreCandidate{
		candidate("big.sqlite", time.Minute, 5000),
		candidate("small.sqlite", 2*time.Minute, 10),
	}, retention)
	if !equalStrings(keep, []string{"big.sqlite"}) || !equalStrings(evict, []string{"small.sqlite"}) {
		t.Fatalf("newest over budget: kept %v evicted %v; the newest store is always kept", keep, evict)
	}
}

// A reader holding a store open keeps reading it across an eviction. POSIX
// unlinks the name and leaves the open inode readable; Windows refuses to
// delete an open file, and the pass skips that store without failing.
func TestSearchStoreRetentionReaderHoldingStoreKeepsReading(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "0")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	heldCache := newCache("gh/acme/widget", "prefix:", "tree-1")
	held := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), heldCache)
	ageRetentionTestStore(t, held, time.Now().Add(-time.Hour))
	publishRetentionTestStore(t, cacheDir, retentionTestKey(2), newCache("gh/acme/widget", "prefix:", "tree-2"))

	db, err := sql.Open("sqlite", "file:"+sqliteURLPath(held)+"?immutable=1&mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// One connection, opened before the eviction by reading only the meta
	// table, so the symbol rows read afterwards come from the open file and
	// not from a fresh open or the connection's page cache.
	db.SetMaxOpenConns(1)
	var version string
	if err := readSearchStoreMetaKey(t.Context(), db, "store_version", &version); err != nil || version != searchStoreVersion {
		t.Fatalf("premise: reader must open the store, got %q (%v)", version, err)
	}

	directory, directoryPath := openRetentionTestDirectory(t, cacheDir)
	result, err := evictSearchStoreSidecars(t.Context(), directory, directoryPath, searchStoreRetention{
		keep: 1, maxBytes: searchStoreRetainBytes,
		group:  searchStoreRetentionGroup("gh/acme/widget", "prefix:"),
		remove: removeSearchStoreSidecar,
	})
	if err != nil {
		t.Fatalf("eviction must not fail on a store a reader holds: %v", err)
	}
	_, statErr := os.Stat(held)
	if runtime.GOOS == "windows" {
		if statErr != nil || result.Skipped[filepath.Base(held)] == nil {
			t.Fatalf("windows: an open store must be skipped and survive, stat=%v result=%+v", statErr, result)
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) || len(result.Evicted) != 1 {
		t.Fatalf("posix: the held store's name must be unlinked, stat=%v result=%+v", statErr, result)
	}
	var symbols int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM brain_symbols_v1").Scan(&symbols); err != nil || symbols != len(heldCache.Snapshot.Symbols) || symbols == 0 {
		t.Fatalf("a reader holding the store must keep reading it after eviction: got %d symbols (%v), want %d", symbols, err, len(heldCache.Snapshot.Symbols))
	}
}

// A reader holding the current store does not block the next publish: the
// republished entry gets its store under a new path, and the held connection
// keeps reading the store it opened. Break caught: publishing by renaming over
// the held file, which Windows refuses ("Access is denied") and which leaves
// the descriptor's path unchanged on every platform.
func TestSearchStorePublishesWhileAReaderHoldsTheStore(t *testing.T) {
	fixture := indexFocusFixture(t)
	before, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+sqliteURLPath(before.Path)+"?immutable=1&mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	var version string
	if err := readSearchStoreMetaKey(t.Context(), db, "store_version", &version); err != nil || version != searchStoreVersion {
		t.Fatalf("premise: the reader must hold the store open, got %q (%v)", version, err)
	}

	commitSameTree(t, fixture.repo)
	head := gitInput(t, fixture.repo, "", "rev-parse", "HEAD")
	forced := fixture.options
	forced.ForceRebuild = true
	snapshot, _, err := PreindexProviderSnapshot(t.Context(), fixture.repo, fixture.version, forced, fixture.cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, warning := range snapshot.Header.Warnings {
		if warning.Code == SearchStoreUnavailableCode {
			t.Fatalf("the publish failed while a reader held the store: %s", warning.Detail)
		}
	}
	after, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	if after.Commit != head || after.Path == before.Path {
		t.Fatalf("the republished store must be described for %s at a new path, got commit %s path %s (was %s)", head, after.Commit, after.Path, before.Path)
	}
	var symbols int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM brain_symbols_v1").Scan(&symbols); err != nil || symbols != before.Symbols || symbols == 0 {
		t.Fatalf("the held connection must keep reading its store: got %d symbols (%v), want %d", symbols, err, before.Symbols)
	}
	if runtime.GOOS != "windows" {
		if _, err := os.Lstat(before.Path); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("POSIX: the superseded store must be unlinked by the publish, got %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := PreindexProviderSnapshot(t.Context(), fixture.repo, fixture.version, forced, fixture.cacheDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(before.Path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the superseded store must be gone once its reader closed and another publish ran, got %v", err)
	}
}

// The current store is the one recorded against the entry now on disk,
// whatever the clocks say. After a backward clock step leaves the superseded
// store newer by modification time, a lookup still takes the current one.
// Break caught: a resolver that picks the newest store by modification time,
// which here is the superseded store, built from other bytes: a miss.
func TestSearchStoreCurrentGenerationSurvivesABackwardClockStep(t *testing.T) {
	fixture := indexFocusFixture(t)
	first, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the superseded store through the republish, as a reader holding it
	// on Windows would.
	removeSearchStoreSidecar = func(directory *os.Root, name string) error {
		if name == filepath.Base(first.Path) {
			return &fs.PathError{Op: "remove", Path: name, Err: errors.New("held by a reader")}
		}
		return directory.Remove(name)
	}
	t.Cleanup(func() { removeSearchStoreSidecar = (*os.Root).Remove })

	commitSameTree(t, fixture.repo)
	forced := fixture.options
	forced.ForceRebuild = true
	if _, _, err := PreindexProviderSnapshot(t.Context(), fixture.repo, fixture.version, forced, fixture.cacheDir); err != nil {
		t.Fatal(err)
	}
	second, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	if got := searchStoreFilesForTest(t, fixture.cacheDir); !equalStrings(got, sortedPaths([]string{first.Path, second.Path})) {
		t.Fatalf("premise: both stores must be on disk, got %v (first %s, second %s)", got, first.Path, second.Path)
	}
	firstInfo, err := os.Stat(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	earlier := firstInfo.ModTime().Add(-time.Hour)
	if err := os.Chtimes(second.Path, earlier, earlier); err != nil {
		t.Fatal(err)
	}

	_, focused, _, err := LoadFocusedProviderSnapshot(t.Context(), fixture.repo, fixture.version,
		fixture.options, fixture.cacheDir, false, leafCallsFocus)
	if err != nil {
		t.Fatal(err)
	}
	if !focused {
		t.Fatal("after a backward clock step the lookup missed the current store")
	}

	// The next publish, of another key in the same cache version, sweeps this
	// key too: it keeps the current store and removes the superseded one.
	removeSearchStoreSidecar = (*os.Root).Remove
	newCache := retentionTestCache(t)
	other := publishSearchStoreAt(t, fixture.cacheDir, searchSnapshotCacheVersion, retentionTestKey(1), newCache("gh/acme/widget", "prefix:", "tree-1"))
	if got := searchStoreFilesForTest(t, fixture.cacheDir); !equalStrings(got, sortedPaths([]string{second.Path, other})) {
		t.Fatalf("the sweep must keep the current store and remove the superseded one, got %v (current %s)", got, second.Path)
	}
}

// A removal the platform refuses (Windows: another process has the store open)
// skips that store, evicts the rest, and never fails the publish.
func TestSearchStoreRetentionSkipsStoreThePlatformCannotDelete(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "0")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	oldest := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), newCache("gh/acme/widget", "prefix:", "tree-1"))
	ageRetentionTestStore(t, oldest, base)
	held := publishRetentionTestStore(t, cacheDir, retentionTestKey(2), newCache("gh/acme/widget", "prefix:", "tree-2"))
	ageRetentionTestStore(t, held, base.Add(time.Minute))

	sharingViolation := errors.New("The process cannot access the file because it is being used by another process.")
	var refused atomic.Int32
	removeSearchStoreSidecar = func(directory *os.Root, name string) error {
		if name == filepath.Base(held) {
			refused.Add(1)
			return &fs.PathError{Op: "remove", Path: name, Err: sharingViolation}
		}
		return directory.Remove(name)
	}
	t.Cleanup(func() { removeSearchStoreSidecar = (*os.Root).Remove })

	// The undeletable store sorts first among those evicted, so the pass has
	// to carry on past the refusal to evict the oldest.
	t.Setenv(searchStoreKeepEnv, "1")
	p3 := publishRetentionTestStore(t, cacheDir, retentionTestKey(3), newCache("gh/acme/widget", "prefix:", "tree-3"))
	if refused.Load() == 0 {
		t.Fatal("premise: the pass never tried to remove the held store")
	}
	if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{held, p3}); !equalStrings(got, want) {
		t.Fatalf("the undeletable store must survive and the pass must still evict the rest: got %v want %v", got, want)
	}
}

// A store republished while another publish's eviction pass has already
// chosen its key survives that pass: the pass removes only the store names it
// listed, and the republish lands under a new name. The fixture forces the
// interleaving: the writer reaches its publish while the pass sits between
// choosing and removing.
func TestSearchStoreRetentionRepublishDuringEvictionSurvives(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "1")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	target := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), newCache("gh/acme/widget", "prefix:", "tree-1"))
	ageRetentionTestStore(t, target, time.Now().Add(-time.Hour))

	var armed atomic.Bool
	aboutToPublish := make(chan struct{})
	published := make(chan struct{})
	beforeSearchStorePublish = func() error {
		if armed.Load() {
			close(aboutToPublish)
		}
		return nil
	}
	afterSearchStorePublish = func() {
		if armed.Load() {
			close(published)
		}
	}
	writerDone := make(chan error, 1)
	var republishedDigest string
	var once sync.Once
	beforeSearchStoreEvictionRemove = func() {
		once.Do(func() {
			armed.Store(true)
			go func() {
				writerDone <- func() error {
					entry, err := newCacheEntry(cacheDir, "search", retentionTestVersion, retentionTestKey(1))
					if err != nil {
						return err
					}
					republish := *newCache("gh/acme/widget", "prefix:", "tree-1")
					republish.Commit = "commit-republished"
					if err := writeSearchSnapshot(entry, republish); err != nil {
						return err
					}
					digest, err := entry.digest()
					if err != nil {
						return err
					}
					republishedDigest = digest.String()
					return writeSearchStoreSidecar(t.Context(), entry, republishedDigest, &republish)
				}()
			}()
			select {
			case <-aboutToPublish:
			case <-time.After(10 * time.Second):
			}
			select {
			case <-published:
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
	t.Cleanup(func() {
		beforeSearchStorePublish, afterSearchStorePublish, beforeSearchStoreEvictionRemove = nil, nil, nil
	})

	writeAndPublishSearchStore(t, cacheDir, retentionTestVersion, retentionTestKey(2), newCache("gh/acme/widget", "prefix:", "tree-2"))
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	for _, path := range retentionTestStores(t, cacheDir) {
		db, err := sql.Open("sqlite", "file:"+sqliteURLPath(path)+"?immutable=1&mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		var digest string
		readErr := readSearchStoreMetaKey(t.Context(), db, "entry_digest", &digest)
		_ = db.Close()
		if readErr == nil && digest == republishedDigest {
			return
		}
	}
	t.Fatalf("the store republished during the eviction pass was removed by it (want a store recording %s, have %v)", republishedDigest, retentionTestStores(t, cacheDir))
}

// Concurrent publishers into one group, run under -race in CI's local gate:
// nothing fails and no temporary is left behind. After one more publish the
// window holds exactly the newest K stores by publish time, so no concurrent
// pass evicted one of the newest.
func TestSearchStoreRetentionConcurrentPublishers(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "2")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()

	var mu sync.Mutex
	publishedAt := map[string]time.Time{}
	afterSearchStorePublish = func() {
		mu.Lock()
		defer mu.Unlock()
		for _, path := range searchStoreFilesForTest(t, cacheDir) {
			if _, seen := publishedAt[path]; seen {
				continue
			}
			if info, err := os.Stat(path); err == nil {
				publishedAt[path] = info.ModTime()
			}
		}
	}
	t.Cleanup(func() { afterSearchStorePublish = nil })

	var group sync.WaitGroup
	errs := make(chan error, 6)
	for index := 1; index <= 6; index++ {
		group.Go(func() {
			entry, err := newCacheEntry(cacheDir, "search", retentionTestVersion, retentionTestKey(index))
			if err == nil {
				cache := newCache("gh/acme/widget", "prefix:", fmt.Sprintf("tree-%d", index))
				if err = writeSearchSnapshot(entry, *cache); err == nil {
					var digest cacheDigest
					if digest, err = entry.digest(); err == nil {
						err = writeSearchStoreSidecar(t.Context(), entry, digest.String(), cache)
					}
				}
			}
			errs <- err
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	newest := ""
	for path, at := range publishedAt {
		if newest == "" || at.After(publishedAt[newest]) || (at.Equal(publishedAt[newest]) && path < newest) {
			newest = path
		}
	}
	if newest == "" {
		t.Fatal("premise: no publish was observed")
	}

	last := publishRetentionTestStore(t, cacheDir, retentionTestKey(7), newCache("gh/acme/widget", "prefix:", "tree-7"))
	if got, want := retentionTestStores(t, cacheDir), sortedPaths([]string{newest, last}); !equalStrings(got, want) {
		t.Fatalf("want the newest concurrent store and the last one, got %v want %v", got, want)
	}
	var temporaries []string
	_ = filepath.WalkDir(cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".tmp") {
			temporaries = append(temporaries, path)
		}
		return err
	})
	if len(temporaries) != 0 {
		t.Fatalf("temporaries left behind: %v", temporaries)
	}
}

// ENTIRE_GRAPH_SEARCH_STORE_KEEP follows the repository's override convention:
// unset or unparseable keeps the default, 0 or negative turns retention off.
func TestSearchStoreRetentionKeepOverride(t *testing.T) {
	for _, tc := range []struct {
		value string
		set   bool
		want  int
	}{
		{set: false, want: defaultSearchStoreKeep},
		{value: "5", set: true, want: 5},
		{value: " 2 ", set: true, want: 2},
		{value: "0", set: true, want: 0},
		{value: "-1", set: true, want: 0},
		{value: "three", set: true, want: defaultSearchStoreKeep},
	} {
		if tc.set {
			t.Setenv(searchStoreKeepEnv, tc.value)
		} else {
			t.Setenv(searchStoreKeepEnv, "")
			if err := os.Unsetenv(searchStoreKeepEnv); err != nil {
				t.Fatal(err)
			}
		}
		if got := resolveSearchStoreKeep(); got != tc.want {
			t.Fatalf("%s=%q (set=%v): got %d want %d", searchStoreKeepEnv, tc.value, tc.set, got, tc.want)
		}
	}

	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "0")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	for index := 1; index <= 4; index++ {
		publishRetentionTestStore(t, cacheDir, retentionTestKey(index), newCache("gh/acme/widget", "prefix:", fmt.Sprintf("tree-%d", index)))
	}
	if got := retentionTestStores(t, cacheDir); len(got) != 4 {
		t.Fatalf("retention off must evict nothing, got %v", got)
	}
}

// Republishing one entry leaves one store, even with the recency window off:
// superseded stores go on every publish, whatever
// ENTIRE_GRAPH_SEARCH_STORE_KEEP says. Break caught: superseded stores
// accumulating, one store-sized leak per republish.
func TestSearchStoreRepublishLeavesOneStoreAtKeepZero(t *testing.T) {
	t.Setenv(searchStoreKeepEnv, "0")
	fixture := indexFocusFixture(t)
	forced := fixture.options
	forced.ForceRebuild = true
	for range 3 {
		if _, _, err := PreindexProviderSnapshot(t.Context(), fixture.repo, fixture.version, forced, fixture.cacheDir); err != nil {
			t.Fatal(err)
		}
	}
	if got := searchStoreFilesForTest(t, fixture.cacheDir); len(got) != 1 {
		t.Fatalf("want the current store only, got %v", got)
	}
}

// A superseded store the platform refuses to delete (Windows: a reader holds
// it) is removed by the next publish. Break caught: a refused removal that is
// never retried, leaking one store per held republish.
func TestSearchStoreSupersededRemovalIsRetried(t *testing.T) {
	t.Setenv(searchStoreEnv, "1")
	t.Setenv(searchStoreKeepEnv, "0")
	newCache := retentionTestCache(t)
	cacheDir := t.TempDir()
	cache := newCache("gh/acme/widget", "prefix:", "tree-1")
	held := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), cache)

	var refused atomic.Int32
	removeSearchStoreSidecar = func(directory *os.Root, name string) error {
		if name == filepath.Base(held) {
			refused.Add(1)
			return &fs.PathError{Op: "remove", Path: name, Err: errors.New("The process cannot access the file because it is being used by another process.")}
		}
		return directory.Remove(name)
	}
	t.Cleanup(func() { removeSearchStoreSidecar = (*os.Root).Remove })

	second := *cache
	second.Commit = "commit-2"
	publishRetentionTestStore(t, cacheDir, retentionTestKey(1), &second)
	if refused.Load() == 0 {
		t.Fatal("premise: the publish never tried to remove the superseded store")
	}

	removeSearchStoreSidecar = (*os.Root).Remove
	third := *cache
	third.Commit = "commit-3"
	latest := publishRetentionTestStore(t, cacheDir, retentionTestKey(1), &third)
	if got := retentionTestStores(t, cacheDir); !equalStrings(got, []string{latest}) {
		t.Fatalf("the next publish must retry the refused removal and leave the current store only, got %v (current %s)", got, latest)
	}
}
