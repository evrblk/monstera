package playground

import (
	"encoding/binary"
	"encoding/gob"
	"io"

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

func (c *PlaygroundCore) Read(request []byte) []byte {
	r, ok := c.state[binary.BigEndian.Uint64(request)]
	if !ok {
		return []byte{}
	}
	return []byte(r)
}

func (c *PlaygroundCore) Update(request []byte) []byte {
	key := binary.BigEndian.Uint64(request[:8])
	value := string(request[8:])
	c.state[key] = value
	return []byte(value)
}

func (c *PlaygroundCore) Snapshot() monstera.ApplicationCoreSnapshot {
	clone := make(map[uint64]string)
	for k, v := range c.state {
		clone[k] = v
	}

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
