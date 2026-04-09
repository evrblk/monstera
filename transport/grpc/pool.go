package grpc

import (
	"log"
	sync "sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcClientPool is a generic pool for gRPC clients.
type GrpcClientPool[T any] struct {
	mu            sync.Mutex
	conns         map[string]*grpcClientEntry[T]
	clientFactory func(*grpc.ClientConn) T
}

func (p *GrpcClientPool[T]) GetConnection(address string) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.conns[address]
	if !ok {
		clientConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			var zero T
			return zero, err
		}
		entry = &grpcClientEntry[T]{
			clientConn: clientConn,
			client:     p.clientFactory(clientConn),
		}
		p.conns[address] = entry
	}
	return entry.client, nil
}

func (p *GrpcClientPool[T]) DeleteConnection(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.conns[address]
	if !ok {
		return
	}
	entry.clientConn.Close()
	delete(p.conns, address)
}

func (p *GrpcClientPool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, entry := range p.conns {
		if err := entry.clientConn.Close(); err != nil {
			log.Printf("error while closing connection: %v", err)
		}
	}
}

// grpcClientEntry holds the gRPC client connection alongside the generic
// client T so that the raw ClientConn can be closed when the entry is removed.
type grpcClientEntry[T any] struct {
	clientConn *grpc.ClientConn
	client     T
}

func NewGrpcClientPool[T any](clientFactory func(*grpc.ClientConn) T) *GrpcClientPool[T] {
	return &GrpcClientPool[T]{
		conns:         make(map[string]*grpcClientEntry[T]),
		clientFactory: clientFactory,
	}
}
