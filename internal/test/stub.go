package test

import (
	"context"
	"encoding/binary"

	"github.com/evrblk/monstera"
)

type PlaygroundApiMonsteraStub struct {
	monsteraClient *monstera.MonsteraClient
}

func (s *PlaygroundApiMonsteraStub) Read(ctx context.Context, key uint64) (string, error) {
	shardKey := monstera.GetShardKey(monstera.ConcatBytes(key), 4)
	request := createKeyBytes(key)

	responseBytes, err := s.monsteraClient.Read(ctx, "Core", shardKey, false, request)

	return string(responseBytes), err
}

func (s *PlaygroundApiMonsteraStub) Update(ctx context.Context, key uint64, value string) (string, error) {
	shardKey := monstera.GetShardKey(monstera.ConcatBytes(key), 4)
	request := createRequestBytes(key, value)

	responseBytes, err := s.monsteraClient.Update(ctx, "Core", shardKey, request)

	return string(responseBytes), err
}

func NewPlaygroundApiMonsteraStub(monsteraClient *monstera.MonsteraClient) *PlaygroundApiMonsteraStub {
	return &PlaygroundApiMonsteraStub{
		monsteraClient: monsteraClient,
	}
}

// createRequestBytes creates a request byte array with key and value
func createRequestBytes(key uint64, value string) []byte {
	request := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(request[:8], key)
	copy(request[8:], value)
	return request
}

// createKeyBytes creates a byte array for a key
func createKeyBytes(key uint64) []byte {
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, key)
	return keyBytes
}
