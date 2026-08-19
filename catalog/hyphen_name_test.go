package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateCatalogHyphenatedName(t *testing.T) {
	dir := t.TempDir()
	prov, err := NewDBProvider("", dir, "myduck")
	require.NoError(t, err)
	defer prov.Close()

	err = prov.CreateCatalog("foo-bar", false)
	require.NoError(t, err, "CREATE/ATTACH catalog with hyphen must succeed")

	_, err = os.Stat(filepath.Join(dir, "foo-bar.db"))
	require.NoError(t, err, "hyphenated catalog file should exist")

	err = prov.CreateCatalog("foo-bar", true)
	require.NoError(t, err, "ATTACH IF NOT EXISTS with hyphen must succeed")

	err = prov.DropCatalog("foo-bar", false)
	require.NoError(t, err, "DETACH/DROP catalog with hyphen must succeed")

	_, err = os.Stat(filepath.Join(dir, "foo-bar.db"))
	require.True(t, os.IsNotExist(err), "dropped hyphenated catalog file should be gone")
}

func TestAttachCatalogHyphenatedName(t *testing.T) {
	dir := t.TempDir()
	prov, err := NewDBProvider("", dir, "myduck")
	require.NoError(t, err)
	defer prov.Close()

	require.NoError(t, prov.CreateCatalog("app-db", false))
	require.NoError(t, prov.DropCatalog("app-db", false))

	// Recreate the file by CreateCatalog then detach without deleting? Drop deletes file.
	// Create again and attach via AttachCatalog after closing/reopening would re-scan.
	require.NoError(t, prov.CreateCatalog("app-db", false))
	info, err := os.Stat(filepath.Join(dir, "app-db.db"))
	require.NoError(t, err)
	require.NoError(t, prov.AttachCatalog(info, false))
}
