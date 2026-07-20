package testcore

import (
	"io"

	"github.com/evrblk/monstera"
)

// NopCore is a trivial ApplicationCore that ignores payloads. Tests that only need
// committed Raft entries (not real application state) use it.
type NopCore struct{}

var _ monstera.ApplicationCore = NopCore{}

func (NopCore) Read(req []byte) (*monstera.ReadResponse, error) {
	return &monstera.ReadResponse{}, nil
}

func (NopCore) Update(req []byte) (*monstera.UpdateResponse, error) {
	return &monstera.UpdateResponse{}, nil
}

func (NopCore) Snapshot() monstera.ApplicationCoreSnapshot { return NopSnapshot{} }

func (NopCore) Restore(r io.ReadCloser) error { return nil }

func (NopCore) Close() {}

type NopSnapshot struct{}

func (NopSnapshot) Write(w io.Writer) error { return nil }

func (NopSnapshot) Release() {}
