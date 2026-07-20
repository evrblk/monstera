package monstera

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticNodeDiscovery(t *testing.T) {
	d := NewStaticNodeDiscovery([]string{"a:1", "b:2"})
	got, err := d.Endpoints(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"a:1", "b:2"}, got)

	// Returned slice is a copy: mutating it must not affect the source.
	got[0] = "mutated"
	got2, err := d.Endpoints(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"a:1", "b:2"}, got2)
}

func TestFileNodeDiscovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.txt")
	content := "# comment\nhost1:9001\n\n  host2:9002  \n# another\nhost3:9003\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	d := NewFileNodeDiscovery(path)
	got, err := d.Endpoints(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"host1:9001", "host2:9002", "host3:9003"}, got)
}

func TestFileNodeDiscoveryMissingFile(t *testing.T) {
	d := NewFileNodeDiscovery(filepath.Join(t.TempDir(), "does-not-exist.txt"))
	_, err := d.Endpoints(context.Background())
	require.Error(t, err)
}
