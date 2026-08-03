package etcdrouteproxy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Router holds etcd client connections and routes keys to the appropriate backend.
type Router struct {
	table   *routeTable
	clients map[string]*clientv3.Client // endpoint URL → client
	logger  *zap.Logger
	mu      sync.RWMutex
}

// TLSConfig holds the TLS configuration shared across all backends.
type TLSConfig struct {
	CertFile      string
	KeyFile       string
	TrustedCAFile string
}

// NewRouter creates a Router from the given config, establishing connections
// to all backend etcd endpoints.
func NewRouter(cfg *Config, tlsCfg *TLSConfig, logger *zap.Logger) (*Router, error) {
	table, err := buildRouteTable(cfg)
	if err != nil {
		return nil, fmt.Errorf("building route table: %w", err)
	}

	// Collect unique endpoints.
	endpoints := map[string]bool{cfg.DefaultEndpoint: true}
	for _, route := range cfg.Routes {
		endpoints[route.Endpoint] = true
	}

	goTLS, err := buildTLSConfig(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("building TLS config: %w", err)
	}

	clients := make(map[string]*clientv3.Client)
	for ep := range endpoints {
		c, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			TLS:         goTLS,
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{grpc.WithBlock()},
			Logger:      logger.Named("etcd-client"),
		})
		if err != nil {
			// Close any already-opened clients.
			for _, opened := range clients {
				opened.Close()
			}
			return nil, fmt.Errorf("connecting to etcd endpoint %s: %w", ep, err)
		}
		clients[ep] = c
	}

	logger.Info("router initialized",
		zap.String("defaultEndpoint", cfg.DefaultEndpoint),
		zap.Int("routes", len(table.prefixToBackend)),
		zap.Int("backends", len(clients)),
	)
	for prefix, ep := range table.prefixToBackend {
		logger.Info("route configured", zap.String("prefix", prefix), zap.String("endpoint", ep))
	}

	return &Router{
		table:   table,
		clients: clients,
		logger:  logger,
	}, nil
}

// Route returns the etcd client for the given key.
func (r *Router) Route(key []byte) *clientv3.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[r.routeToEndpoint(key)]
}

// routeToEndpoint returns the endpoint URL that the given key should be
// routed to, based on prefix matching against the route table.
func (r *Router) routeToEndpoint(key []byte) string {
	keyStr := string(key)
	for prefix, ep := range r.table.prefixToBackend {
		if strings.HasPrefix(keyStr, prefix) {
			return ep
		}
	}
	return r.table.defaultBackend
}

// DefaultClient returns the client for the default backend.
func (r *Router) DefaultClient() *clientv3.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[r.table.defaultBackend]
}

// AllClients returns all backend clients (for operations like Compact
// that must be broadcast).
func (r *Router) AllClients() []*clientv3.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[*clientv3.Client]bool)
	var result []*clientv3.Client
	for _, c := range r.clients {
		if !seen[c] {
			seen[c] = true
			result = append(result, c)
		}
	}
	return result
}

// Close closes all backend connections.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for ep, c := range r.clients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing client for %s: %w", ep, err)
		}
	}
	return firstErr
}

func buildTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	if cfg == nil || (cfg.CertFile == "" && cfg.KeyFile == "" && cfg.TrustedCAFile == "") {
		return nil, nil
	}

	tlsConfig := &tls.Config{}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if cfg.TrustedCAFile != "" {
		caCert, err := os.ReadFile(cfg.TrustedCAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", cfg.TrustedCAFile)
		}
		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}
