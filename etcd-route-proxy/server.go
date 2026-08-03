package etcdrouteproxy

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server is the etcd route proxy gRPC server.
type Server struct {
	grpcServer *grpc.Server
	router     *Router
	logger     *zap.Logger
	listeners  []net.Listener
}

// ServerConfig holds configuration for the proxy server.
type ServerConfig struct {
	// ListenClientURLs are the URLs to listen on (matching etcd's --listen-client-urls).
	ListenClientURLs []string

	// CertFile is the TLS certificate for the proxy's serving endpoint.
	CertFile string
	// KeyFile is the TLS key for the proxy's serving endpoint.
	KeyFile string
	// TrustedCAFile is the CA used to verify client connections (if mTLS is desired)
	// and to connect to backend etcd instances.
	TrustedCAFile string
}

// NewServer creates a new proxy server.
func NewServer(cfg *ServerConfig, routingCfg *Config, logger *zap.Logger) (*Server, error) {
	// Build TLS config for backend connections (client TLS).
	backendTLS := &TLSConfig{
		CertFile:      cfg.CertFile,
		KeyFile:       cfg.KeyFile,
		TrustedCAFile: cfg.TrustedCAFile,
	}

	router, err := NewRouter(routingCfg, backendTLS, logger)
	if err != nil {
		return nil, fmt.Errorf("creating router: %w", err)
	}

	// Build serving TLS config.
	var grpcOpts []grpc.ServerOption
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading serving cert/key: %w", err)
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.NoClientCert,
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	grpcServer := grpc.NewServer(grpcOpts...)

	// Register all etcd gRPC services.
	pb.RegisterKVServer(grpcServer, newKVProxy(router, logger.Named("kv")))
	pb.RegisterWatchServer(grpcServer, newWatchProxy(router, logger.Named("watch")))
	pb.RegisterLeaseServer(grpcServer, newLeaseProxy(router, logger.Named("lease")))
	pb.RegisterMaintenanceServer(grpcServer, newMaintenanceProxy(router, logger.Named("maintenance")))

	// Register gRPC health service (used by etcd client health checks).
	hsrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	return &Server{
		grpcServer: grpcServer,
		router:     router,
		logger:     logger,
	}, nil
}

// Serve starts listening on all configured URLs and blocks until stopped.
func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listeners[0])
}

// Listen creates listeners for all configured URLs.
func (s *Server) Listen(urls []string) error {
	for _, u := range urls {
		parsed, err := url.Parse(u)
		if err != nil {
			return fmt.Errorf("parsing listen URL %q: %w", u, err)
		}
		host := parsed.Host
		if host == "" {
			host = parsed.Path // handle "0.0.0.0:2379" without scheme
		}
		lis, err := net.Listen("tcp", host)
		if err != nil {
			return fmt.Errorf("listening on %s: %w", host, err)
		}
		s.listeners = append(s.listeners, lis)
		s.logger.Info("listening", zap.String("address", lis.Addr().String()))
	}
	if len(s.listeners) == 0 {
		return fmt.Errorf("no listen URLs configured")
	}

	// If multiple listeners, serve additional ones in goroutines.
	for _, lis := range s.listeners[1:] {
		go func(l net.Listener) {
			if err := s.grpcServer.Serve(l); err != nil {
				s.logger.Error("grpc serve error", zap.Error(err))
			}
		}(lis)
	}
	return nil
}

// Stop gracefully stops the server and closes backend connections.
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
	if err := s.router.Close(); err != nil {
		s.logger.Error("error closing router", zap.Error(err))
	}
}
