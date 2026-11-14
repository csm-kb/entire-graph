package sem

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// A search store is published write-once. Each cache key owns a directory,
// search/<version>/<key>.sqlite.d/, and every build of the key's store is
// published there once, under a fresh random name (a generation,
// <16hex>.sqlite), and is never modified, renamed or renamed over afterwards.
// Windows cannot rename over or delete a file a SQLite reader holds open
// (modernc shares only read and write), so a fixed name made every publish
// depend on every reader having closed. The key directory itself is never
// removed. <key>/ is not available for this: it holds derived entries, which
// `index --force` deletes.
//
// Which generation is current is decided by content first
// (orderSearchStoreGenerations): a generation that recorded the stamp of the
// gz entry now on disk mirrors exactly those bytes, whatever the clocks say.
const searchStoreKeyDirSuffix = ".sqlite.d"

// searchStoreKeyDirFor is the ONE place a store's directory is derived from
// its entry: the entry's relative path with .sqlite.d replacing .json.gz.
// Writer and readers address a key's stores through it.
func searchStoreKeyDirFor(entry cacheEntry) cacheEntry {
	keyDir := entry
	keyDir.relative = strings.TrimSuffix(entry.relative, ".json.gz") + searchStoreKeyDirSuffix
	return keyDir
}

// searchStoreKeyDirName reports whether name is a key directory: a cache key
// followed by .sqlite.d.
func searchStoreKeyDirName(name string) bool {
	key, ok := strings.CutSuffix(name, searchStoreKeyDirSuffix)
	return ok && validSHA256Hex(key)
}

// searchStoreGenerationName reports whether name is a published generation:
// 16 lowercase hex digits followed by .sqlite.
func searchStoreGenerationName(name string) bool {
	stem, ok := strings.CutSuffix(name, ".sqlite")
	return ok && isLowerHex(stem, 16)
}

func isLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for index := 0; index < len(value); index++ {
		if (value[index] < '0' || value[index] > '9') && (value[index] < 'a' || value[index] > 'f') {
			return false
		}
	}
	return true
}

// newSearchStoreGenerationName draws a fresh generation name. A collision
// with an existing generation is negligible at one to three generations per
// key, and the publish does not check for one.
func newSearchStoreGenerationName() string {
	return fmt.Sprintf("%016x.sqlite", rand.Uint64())
}

// createSearchStoreTemp creates an empty temporary for a store build in the
// key directory, opened with O_EXCL. The name is short on purpose: modernc
// gets a plain path with no \\?\ prefix, and the key directory already adds
// 26 characters to the longest path under the cache.
func createSearchStoreTemp(keyDir *os.Root) (*os.File, string, error) {
	for attempt := 0; attempt < 16; attempt++ {
		name := fmt.Sprintf(".%016x.tmp", rand.Uint64())
		file, err := keyDir.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return file, name, nil
	}
	return nil, "", fmt.Errorf("create temporary store in %s: no unused name", keyDir.Name())
}

// searchStoreGeneration is one published store of a key.
type searchStoreGeneration struct {
	name    string
	path    string
	size    int64
	modTime time.Time
	// stamp is the entry stamp the generation recorded, nil when it has none
	// or it cannot be read.
	stamp *searchStoreEntryStamp
}

// openSearchStoreForRead opens a published store the way every reader does:
// read-only and immutable.
func openSearchStoreForRead(path string) (*sql.DB, error) {
	return sql.Open("sqlite", "file:"+sqliteURLPath(path)+"?immutable=1&mode=ro")
}

// listSearchStoreGenerations returns the generations in the key directory at
// keyDirPath, each with its recorded stamp. A name is kept only when Stat
// reports a regular file, and that check runs before SQLite opens anything: a
// FIFO would block the open beyond the reach of a context. Stat follows links,
// as store readers always have, so a link to a FIFO is rejected too.
func listSearchStoreGenerations(ctx context.Context, keyDirPath string) ([]searchStoreGeneration, error) {
	entries, err := os.ReadDir(keyDirPath)
	if err != nil {
		return nil, err
	}
	var generations []searchStoreGeneration
	for _, entry := range entries {
		name := entry.Name()
		if !searchStoreGenerationName(name) {
			continue
		}
		path := filepath.Join(keyDirPath, name)
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		generations = append(generations, searchStoreGeneration{
			name: name, path: path, size: info.Size(), modTime: info.ModTime(),
			stamp: readSearchStoreRecordedStamp(ctx, path),
		})
	}
	return generations, nil
}

func readSearchStoreRecordedStamp(ctx context.Context, path string) *searchStoreEntryStamp {
	db, err := openSearchStoreForRead(path)
	if err != nil {
		return nil
	}
	defer db.Close()
	var stamp searchStoreEntryStamp
	if err := readSearchStoreMetaKey(ctx, db, "entry_stamp", &stamp); err != nil {
		return nil
	}
	return &stamp
}

// orderSearchStoreGenerations puts the key's current generation first. The
// order is: a generation whose recorded stamp equals entryStamp (the gz entry
// on disk now; nil when it could not be stamped), then the newer modification
// time, then the lower name. The resolver serves the first, the superseded
// sweep (sweepSupersededSearchStores) keeps it, and the retention window takes
// it as the key's identity, so all three agree whatever the clocks say. The
// tie-break matches planSearchStoreEviction's.
func orderSearchStoreGenerations(generations []searchStoreGeneration, entryStamp *searchStoreEntryStamp) {
	matches := func(generation searchStoreGeneration) bool {
		return entryStamp != nil && generation.stamp != nil && *generation.stamp == *entryStamp
	}
	slices.SortStableFunc(generations, func(a, b searchStoreGeneration) int {
		if matchA, matchB := matches(a), matches(b); matchA != matchB {
			if matchA {
				return -1
			}
			return 1
		}
		if !a.modTime.Equal(b.modTime) {
			if a.modTime.After(b.modTime) {
				return -1
			}
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
}

// searchStoreGenerationRecorded reports whether a generation in the key
// directory already mirrors this entry instance: it recorded stamp (file
// identity included) and entryDigest at this build's store version. A second
// build would only be swept again.
func searchStoreGenerationRecorded(ctx context.Context, keyDirPath string, stamp searchStoreEntryStamp, entryDigest string) bool {
	generations, err := listSearchStoreGenerations(ctx, keyDirPath)
	if err != nil {
		return false
	}
	for _, generation := range generations {
		if generation.stamp == nil || *generation.stamp != stamp {
			continue
		}
		db, err := openSearchStoreForRead(generation.path)
		if err != nil {
			continue
		}
		var version, digest string
		recorded := readSearchStoreMetaKey(ctx, db, "store_version", &version) == nil &&
			readSearchStoreMetaKey(ctx, db, "entry_digest", &digest) == nil &&
			version == searchStoreVersion && digest == entryDigest
		_ = db.Close()
		if recorded {
			return true
		}
	}
	return false
}

// syncSearchStoreFile flushes a built store to stable storage before it is
// published. The build runs with synchronous(OFF), and the meta check reads
// only the meta rows, so without this a crash could leave a published store
// whose data pages are zero or missing, undetected.
func syncSearchStoreFile(directory *os.Root, name string) error {
	file, err := directory.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// searchStoreTempName reports whether name is a store build's temporary: a
// dot, 16 lowercase hex digits, .tmp.
func searchStoreTempName(name string) bool {
	stem, ok := strings.CutSuffix(name, ".tmp")
	return ok && strings.HasPrefix(stem, ".") && isLowerHex(stem[1:], 16)
}

// currentSearchEntryStamp stamps the gz entry on disk now.
func currentSearchEntryStamp(entry cacheEntry) (searchStoreEntryStamp, error) {
	file, err := entry.open()
	if err != nil {
		return searchStoreEntryStamp{}, err
	}
	defer file.Close()
	return searchEntryStampOf(file)
}

// resolveSearchStoreGeneration finds entry's current store: the first of its
// key directory's generations, ordered against the entry's stamp now. It also
// returns that stamp, which a caller compares with the generation's recorded
// one. A key directory that is missing or cannot be listed, an empty one, and
// an entry that cannot be stamped are all ErrSearchStoreMissing, matching the
// quiet miss of any stat error on the store.
func resolveSearchStoreGeneration(ctx context.Context, entry cacheEntry) (searchStoreGeneration, searchStoreEntryStamp, error) {
	keyDir := searchStoreKeyDirFor(entry)
	generations, err := listSearchStoreGenerations(ctx, filepath.Join(keyDir.root, keyDir.relative))
	if err != nil || len(generations) == 0 {
		return searchStoreGeneration{}, searchStoreEntryStamp{}, ErrSearchStoreMissing
	}
	entryStamp, err := currentSearchEntryStamp(entry)
	if err != nil {
		return searchStoreGeneration{}, searchStoreEntryStamp{}, ErrSearchStoreMissing
	}
	orderSearchStoreGenerations(generations, &entryStamp)
	return generations[0], entryStamp, nil
}
