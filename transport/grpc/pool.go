package grpc

import (
	"errors"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrPoolClosed is returned by GetConnection after the pool has been closed.
var ErrPoolClosed = errors.New("grpc client pool is closed")

// GrpcClientPool is a generic pool for gRPC clients.
type GrpcClientPool[T any] struct {
	mu            sync.Mutex
	conns         map[string]*grpcClientEntry[T]
	clientFactory func(*grpc.ClientConn) T
	// dialOptions are applied to every connection created by the pool, on top of
	// the insecure transport credentials. Used to configure keepalive, message
	// sizes, etc.
	dialOptions []grpc.DialOption
	// closed is set by Close. Once set, GetConnection refuses to create new
	// connections so a racing caller cannot leave a live ClientConn in the map
	// after Close has already swept it — that connection would leak.
	closed bool
}

func (p *GrpcClientPool[T]) GetConnection(address string) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		var zero T
		return zero, ErrPoolClosed
	}

	entry, ok := p.conns[address]
	if !ok {
		opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, p.dialOptions...)
		clientConn, err := grpc.NewClient(address, opts...)
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

// Close marks the pool closed (so GetConnection stops handing out or creating
// connections) and closes every pooled connection. It is idempotent.
func (p *GrpcClientPool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	for address, entry := range p.conns {
		if err := entry.clientConn.Close(); err != nil {
			log.Printf("error while closing connection: %v", err)
		}
		delete(p.conns, address)
	}
}

// grpcClientEntry holds the gRPC client connection alongside the generic
// client T so that the raw ClientConn can be closed when the entry is removed.
type grpcClientEntry[T any] struct {
	clientConn *grpc.ClientConn
	client     T
}

func NewGrpcClientPool[T any](clientFactory func(*grpc.ClientConn) T, dialOptions ...grpc.DialOption) *GrpcClientPool[T] {
	return &GrpcClientPool[T]{
		conns:         make(map[string]*grpcClientEntry[T]),
		clientFactory: clientFactory,
		dialOptions:   dialOptions,
	}
}
