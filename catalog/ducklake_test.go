package catalog

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDuckLakeExtensionManifestValidation(t *testing.T) {
	dir := t.TempDir()
	contents := map[string][]byte{
		"httpfs.duckdb_extension":   []byte("httpfs-test"),
		"ducklake.duckdb_extension": []byte("ducklake-test"),
	}
	manifest := make([]ExtensionArtifact, 0, len(contents))
	for name, content := range contents {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(content)
		base := name[:len(name)-len(".duckdb_extension")]
		manifest = append(manifest, ExtensionArtifact{
			Name: base, FileName: name, SHA256: fmt.Sprintf("%x", hash[:]),
			Architecture: "arm64", ABI: DuckDBExtensionABI, Version: DuckDBExtensionVersion,
		})
	}
	if err := VerifyDuckLakeExtensions(dir, manifest); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}

	manifest[0].SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := VerifyDuckLakeExtensions(dir, manifest); err == nil {
		t.Fatal("tampered hash unexpectedly accepted")
	}
}

func TestDuckLakeExtensionManifestRejectsSymlinkAndWrongTarget(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"httpfs.duckdb_extension", "ducklake.duckdb_extension"} {
		if err := os.WriteFile(filepath.Join(dir, name+".real"), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(name+".real", filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := DuckLakeExtensionManifest("linux", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDuckLakeExtensions(dir, manifest); err == nil {
		t.Fatal("symlinked extension unexpectedly accepted")
	}
	if err := VerifyDuckLakeExtensionsForTarget(dir, manifest, "linux", "amd64"); err == nil {
		t.Fatal("wrong architecture unexpectedly accepted")
	}
}
