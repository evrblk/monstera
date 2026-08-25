// Package fsutil holds small, dependency-free filesystem helpers shared across
// the codebase.
package fsutil

import (
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to path atomically and durably. It writes to a
// temporary file in the same directory, fsyncs its contents, renames it over
// path, and fsyncs the directory so the rename entry itself survives a crash.
// The parent directory is created if missing. A crash or power loss mid-write
// therefore never leaves a torn, zero-length, or partially-written file at path:
// a reader afterwards sees either the previous contents or the complete new
// ones, never a mix.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup: a no-op after a successful rename, removes the temp
	// file on any error path.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	// fsync the contents before the rename so a crash can't expose a torn or
	// zero-length file under the destination name.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	// fsync the directory so the rename entry itself survives a crash.
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
