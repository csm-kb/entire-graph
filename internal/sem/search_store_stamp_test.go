package sem

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// A focused lookup takes the gz entry's stamp (size, modification time and
// file identity), recorded in the store beside the digest, as proof the entry
// is the one the store mirrors. Any change to the stamp brings the hash back,
// and the hash catches a changed entry. Break caught: an entry rewritten after
// its store was built is answered from the store as though it were unchanged.
func TestFocusedLookupHashesAnEntryWhoseStampChanged(t *testing.T) {
	fixture := indexFocusFixture(t)
	rewriteEntryChangingItsStamp(t, focusFixtureEntryPath(t, fixture))
	_, focused, _, err := LoadFocusedProviderSnapshot(t.Context(), fixture.repo, fixture.version,
		fixture.options, fixture.cacheDir, false, leafCallsFocus)
	if err != nil {
		t.Fatal(err)
	}
	if focused {
		t.Fatal("the entry's stamp changed, yet the sidecar answered without the hash that would catch its new bytes")
	}
}

// The warm full load (query --head, and a lookup's fallback) checks the
// sidecar against the entry before answering from it. Break caught: the full
// read serving a rewritten entry's sidecar as a cache hit, by skipping that
// check or trusting a recorded stamp or digest the entry no longer has.
func TestFullLoadNeverServesTheSidecarOfAChangedEntry(t *testing.T) {
	fixture := indexFocusFixture(t)
	rewriteEntryChangingItsStamp(t, focusFixtureEntryPath(t, fixture))
	_, cacheHit, err := LoadOrBuildProviderSnapshot(t.Context(), fixture.repo, fixture.version,
		fixture.options, fixture.cacheDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if cacheHit {
		t.Fatal("the entry's stamp changed, yet the sidecar answered as a cache hit without the hash that would catch its new bytes")
	}
}

// rewriteEntryChangingItsStamp replaces the entry's bytes with as many bytes of
// garbage and moves its modification time a second on, so its stamp no longer
// matches the one its sidecar recorded.
func rewriteEntryChangingItsStamp(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, int(info.Size())), 0o600); err != nil {
		t.Fatal(err)
	}
	later := info.ModTime().Add(time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
}

// focusFixtureEntryPath is the gz entry the fixture's index wrote: the one
// *.json.gz file under its cache directory.
func focusFixtureEntryPath(t *testing.T, fixture focusFixture) string {
	t.Helper()
	var entries []string
	if err := filepath.WalkDir(fixture.cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".json.gz") {
			entries = append(entries, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want one complete entry, got %q", entries)
	}
	return entries[0]
}

// An entry replaced by another file of the same size, with its modification
// time put back, is a different entry: the lookup must hash it rather than
// trust the stamp. Break caught: trusting size and modification time alone,
// which serves the replaced entry's store as if it were current.
//
// The setup replaces the cache file directly instead of rewriting the entry
// through Graph, which would also rebuild its store; a rename is the cheap
// way to get a new file with the old size and time.
func TestFocusedLookupHashesAReplacedEntryWithTheSameSizeAndTime(t *testing.T) {
	fixture := indexFocusFixture(t)
	path := focusFixtureEntryPath(t, fixture)
	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(filepath.Dir(path), "replacement.tmp")
	if err := os.WriteFile(replacement, bytes.Repeat([]byte{'x'}, int(original.Size())), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, original.ModTime(), original.ModTime()); err != nil {
		t.Fatal(err)
	}
	// Checked first: the focused miss below falls back to the full load,
	// which may write a fresh store.
	if _, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options); !errors.Is(err, ErrSearchStoreMissing) {
		t.Fatalf("premise: the replaced entry's store must not describe as current, got %v", err)
	}
	_, focused, _, err := LoadFocusedProviderSnapshot(t.Context(), fixture.repo, fixture.version,
		fixture.options, fixture.cacheDir, false, leafCallsFocus)
	if err != nil {
		t.Fatal(err)
	}
	if focused {
		t.Fatal("a replaced entry with the same size and modification time was trusted, and its old store answered")
	}
}

// searchStoreFilesForTest lists every published store file (*.sqlite) under
// root, sorted. It knows nothing of how stores are laid out beneath root.
func searchStoreFilesForTest(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".sqlite") {
			paths = append(paths, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	return paths
}

// commitSameTree makes an empty commit: HEAD moves, while its tree, and with
// it the cache key, stays.
func commitSameTree(t *testing.T, repo string) {
	t.Helper()
	git(t, repo, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "commit", "-q", "--allow-empty", "-m", "same tree")
}
