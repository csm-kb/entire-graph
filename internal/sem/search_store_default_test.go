package sem

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPreindexHitSurfacesStoreRepairFailureAsWarning pins how a no-op index
// treats a sidecar it cannot build now that the sidecar is on by default: the
// index succeeds (an operator who never opted in must not see it fail on, say,
// a full disk) and the failure is a W_SEARCH_STORE_UNAVAILABLE header warning,
// never a silent drop. Break caught: the repair error failing `index` again,
// or being swallowed.
func TestPreindexHitSurfacesStoreRepairFailureAsWarning(t *testing.T) {
	t.Setenv(searchStoreEnv, "0")
	repo := t.TempDir()
	git(t, repo, "init", "-q")
	git(t, repo, "config", "user.name", "Store Default Test")
	git(t, repo, "config", "user.email", "store-default@example.invalid")
	write(t, repo, "main.go", "package sample\n\nfunc Render() {}\n")
	git(t, repo, "add", "-A")
	git(t, repo, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "fixture")
	cacheDir := t.TempDir()
	options := ProviderSnapshotOptions{NoNetwork: true, Profile: ProfileFull}
	const version = "store-repair-warning-test"
	if _, _, err := PreindexProviderSnapshot(t.Context(), repo, version, options, cacheDir); err != nil {
		t.Fatal(err)
	}

	var entries []string
	if err := filepath.WalkDir(cacheDir, func(path string, d fs.DirEntry, err error) error {
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
	// A regular file where the store's key directory belongs: the lookup
	// cannot list it, and the rebuild cannot open it as a directory.
	keyDir := strings.TrimSuffix(entries[0], ".json.gz") + ".sqlite.d"
	if err := os.WriteFile(keyDir, []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}

	os.Unsetenv(searchStoreEnv)
	snapshot, hit, err := PreindexProviderSnapshot(t.Context(), repo, version, options, cacheDir)
	if err != nil {
		t.Fatalf("a sidecar repair failure failed the index: %v", err)
	}
	if !hit {
		t.Fatal("second index was not a cache hit")
	}
	var found *ProviderWarning
	for index := range snapshot.Header.Warnings {
		if snapshot.Header.Warnings[index].Code == SearchStoreUnavailableCode {
			found = &snapshot.Header.Warnings[index]
		}
	}
	if found == nil || found.Detail == "" || found.Severity != "warning" {
		t.Fatalf("repair failure not surfaced as a %s warning: %+v", SearchStoreUnavailableCode, snapshot.Header.Warnings)
	}
}

// A publish that fails leaves the previous store serving the entry it still
// matches. Break caught: the failure path deleting the live store, which sends
// every lookup to the full snapshot load until the next successful index.
func TestFailedStorePublishKeepsThePreviousStore(t *testing.T) {
	fixture := indexFocusFixture(t)
	entryPath := focusFixtureEntryPath(t, fixture)
	entryBefore, _ := fileSHA256(t, entryPath)
	beforeSearchStorePublish = func() error { return errors.New("publish refused by the test") }
	t.Cleanup(func() { beforeSearchStorePublish = nil })

	forced := fixture.options
	forced.ForceRebuild = true
	snapshot, _, err := PreindexProviderSnapshot(t.Context(), fixture.repo, fixture.version, forced, fixture.cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	refused := false
	for _, warning := range snapshot.Header.Warnings {
		refused = refused || warning.Code == SearchStoreUnavailableCode
	}
	if !refused {
		t.Fatal("premise: the publish must have failed")
	}
	if entryAfter, _ := fileSHA256(t, entryPath); entryAfter != entryBefore {
		t.Fatal("premise: a rebuild at the same commit must rewrite the same entry bytes")
	}

	_, focused, _, err := LoadFocusedProviderSnapshot(t.Context(), fixture.repo, fixture.version,
		fixture.options, fixture.cacheDir, false, leafCallsFocus)
	if err != nil {
		t.Fatal(err)
	}
	if !focused {
		t.Fatal("the failed publish removed the store that still matches the entry")
	}
	if _, err := DescribeSearchStore(t.Context(), fixture.repo, fixture.version, fixture.cacheDir, fixture.options); err != nil {
		t.Fatalf("the previous store must still resolve: %v", err)
	}
	if err := filepath.WalkDir(fixture.cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".tmp") {
			t.Errorf("the failed publish left a temporary file: %s", path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
