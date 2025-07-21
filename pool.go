package monstera

import (
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MonsteraConnectionPool struct {
	mu    sync.Mutex
	conns map[string]*grpcConnection
}

type grpcConnection struct {
	clientConn *grpc.ClientConn
	grpcClient MonsteraApiClient
}

func (p *MonsteraConnectionPool) GetConnection(nodeAddress string) (MonsteraApiClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.conns[nodeAddress]
	if !ok {
		clientConn, err := grpc.NewClient(nodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conn = &grpcConnection{
			clientConn: clientConn,
			grpcClient: NewMonsteraApiClient(clientConn),
		}
		p.conns[nodeAddress] = conn
	}
	return conn.grpcClient, nil
}

func (p *MonsteraConnectionPool) DeleteConnection(nodeAddress string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.conns[nodeAddress]
	if !ok {
		return
	}

	conn.clientConn.Close()
	delete(p.conns, nodeAddress)
}

func (p *MonsteraConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.conns {
		err := conn.clientConn.Close()
		if err != nil {
			log.Printf("error while closing connection %v", err)
		}
	}
}

func NewMonsteraConnectionPool() *MonsteraConnectionPool {
	return &MonsteraConnectionPool{
		conns: make(map[string]*grpcConnection),
	}
}
