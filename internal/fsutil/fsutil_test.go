package fsutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomic(t *testing.T) {
	// Target lives in a not-yet-existing subdirectory: WriteFileAtomic must create it.
	path := filepath.Join(t.TempDir(), "sub", "dir", "file.json")

	require.NoError(t, WriteFileAtomic(path, []byte("first"), 0o644))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	// Overwrite: the reader sees the complete new contents, not a mix.
	require.NoError(t, WriteFileAtomic(path, []byte("second-longer"), 0o600))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "second-longer", string(got))

	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	// No temp files linger in the directory after a successful write.
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file.json", entries[0].Name())
}
