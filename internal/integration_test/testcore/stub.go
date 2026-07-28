package testcore

import (
	"context"
	"encoding/binary"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/utils"
)

// PlaygroundApiMonsteraStub is the client-side counterpart of PlaygroundCore: it
// encodes typed key/value calls into the byte payloads PlaygroundCore expects and
// routes them through a monstera.Client.
type PlaygroundApiMonsteraStub struct {
	monsteraClient *monstera.Client
}

func NewPlaygroundApiMonsteraStub(monsteraClient *monstera.Client) *PlaygroundApiMonsteraStub {
	return &PlaygroundApiMonsteraStub{
		monsteraClient: monsteraClient,
	}
}

func (s *PlaygroundApiMonsteraStub) Read(ctx context.Context, key uint64) (string, error) {
	shardKey := utils.GetShardKey(utils.ConcatBytes(key))
	request := createKeyBytes(key)

	responseBytes, err := s.monsteraClient.Read(ctx, "Core", shardKey, false, request)

	return string(responseBytes), err
}

func (s *PlaygroundApiMonsteraStub) Update(ctx context.Context, key uint64, value string) (string, error) {
	shardKey := utils.GetShardKey(utils.ConcatBytes(key))
	request := createRequestBytes(key, value)

	responseBytes, err := s.monsteraClient.Update(ctx, "Core", shardKey, request)

	return string(responseBytes), err
}

// createRequestBytes encodes an update payload: 8-byte big-endian key + value.
func createRequestBytes(key uint64, value string) []byte {
	request := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(request[:8], key)
	copy(request[8:], value)
	return request
}

// createKeyBytes encodes a read payload: 8-byte big-endian key.
func createKeyBytes(key uint64) []byte {
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, key)
	return keyBytes
}
