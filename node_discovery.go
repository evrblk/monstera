package monstera

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// NodeDiscovery answers "which node addresses should I ask for the cluster
// config?". It is deliberately separate from ConfigProvider — who to ask versus
// what they say — so the bootstrap source is pluggable per deployment (a static
// list, a file, a DNS SRV record, ...).
//
// Discovery only needs to be good enough to reach one live node: once a
// PollingClusterConfigProvider has a config, the authoritative node set comes from that
// config, and discovery is just the durable "who do I ask" seed and ongoing
// membership hint. Endpoints is called once per poll round, so implementations
// may re-resolve (DNS, file) on each call.
type NodeDiscovery interface {
	// Endpoints returns candidate gRPC addresses ("host:port").
	Endpoints(ctx context.Context) ([]string, error)
}

// StaticNodeDiscovery returns a fixed list of addresses (e.g. from a flag).
type StaticNodeDiscovery struct {
	addrs []string
}

var _ NodeDiscovery = (*StaticNodeDiscovery)(nil)

func NewStaticNodeDiscovery(addrs []string) *StaticNodeDiscovery {
	return &StaticNodeDiscovery{addrs: append([]string(nil), addrs...)}
}

func (d *StaticNodeDiscovery) Endpoints(ctx context.Context) ([]string, error) {
	return append([]string(nil), d.addrs...), nil
}

// FileNodeDiscovery reads addresses from a file on every call — one "host:port"
// per line; blank lines and lines starting with '#' are ignored. Reading on each
// call lets an operator edit the file to change the candidate set without a
// restart.
type FileNodeDiscovery struct {
	path string
}

var _ NodeDiscovery = (*FileNodeDiscovery)(nil)

func NewFileNodeDiscovery(path string) *FileNodeDiscovery {
	return &FileNodeDiscovery{path: path}
}

func (d *FileNodeDiscovery) Endpoints(ctx context.Context) ([]string, error) {
	data, err := os.ReadFile(d.path)
	if err != nil {
		return nil, fmt.Errorf("reading node discovery file %s: %w", d.path, err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, nil
}

// SRVNodeDiscovery resolves a DNS SRV record to node addresses on every call —
// the natural fit for Kubernetes headless services or Consul, where membership is
// already published in DNS. The name is looked up directly (service and proto are
// empty), so pass the full SRV record name.
type SRVNodeDiscovery struct {
	name     string
	resolver *net.Resolver
}

var _ NodeDiscovery = (*SRVNodeDiscovery)(nil)

func NewSRVNodeDiscovery(name string, resolver *net.Resolver) *SRVNodeDiscovery {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &SRVNodeDiscovery{name: name, resolver: resolver}
}

func (d *SRVNodeDiscovery) Endpoints(ctx context.Context) ([]string, error) {
	_, records, err := d.resolver.LookupSRV(ctx, "", "", d.name)
	if err != nil {
		return nil, fmt.Errorf("SRV lookup %s: %w", d.name, err)
	}
	out := make([]string, 0, len(records))
	for _, r := range records {
		host := strings.TrimSuffix(r.Target, ".")
		out = append(out, net.JoinHostPort(host, strconv.Itoa(int(r.Port))))
	}
	return out, nil
}
