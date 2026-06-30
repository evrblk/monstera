package test

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"maps"

	"github.com/evrblk/monstera"
)

type PlaygroundCore struct {
	state map[uint64]string
}

var _ monstera.ApplicationCore = &PlaygroundCore{}

func (c *PlaygroundCore) Close() {
}

func (c *PlaygroundCore) Restore(snapshot io.ReadCloser) error {
	c.state = make(map[uint64]string)

	dec := gob.NewDecoder(snapshot)
	err := dec.Decode(&c.state)
	if err != nil {
		return err
	}

	return nil
}

func (c *PlaygroundCore) Read(request []byte) (*monstera.ReadResponse, error) {
	r, ok := c.state[binary.BigEndian.Uint64(request)]
	if !ok {
		return &monstera.ReadResponse{
			Data: []byte{},
		}, nil
	}
	return &monstera.ReadResponse{
		Data: []byte(r),
	}, nil
}

func (c *PlaygroundCore) Update(request []byte) (*monstera.UpdateResponse, error) {
	key := binary.BigEndian.Uint64(request[:8])
	value := string(request[8:])
	c.state[key] = value
	return &monstera.UpdateResponse{
		Data: []byte(value),
	}, nil
}

func (c *PlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	clone := make(map[uint64]string)
	maps.Copy(clone, c.state)

	return &PlaygroundCoreSnapshot{
		state: clone,
	}
}

type PlaygroundCoreSnapshot struct {
	state map[uint64]string
}

func (s *PlaygroundCoreSnapshot) Write(w io.Writer) error {
	enc := gob.NewEncoder(w)
	err := enc.Encode(s.state)
	if err != nil {
		return err
	}
	return nil
}

func (s *PlaygroundCoreSnapshot) Release() {
}

func NewPlaygroundCore() *PlaygroundCore {
	return &PlaygroundCore{
		state: make(map[uint64]string),
	}
}
