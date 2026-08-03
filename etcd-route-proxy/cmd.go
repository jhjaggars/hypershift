package etcdrouteproxy

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewStartCommand creates the cobra command for the etcd-route-proxy.
func NewStartCommand() *cobra.Command {
	opts := &options{}

	cmd := &cobra.Command{
		Use:   "etcd-route-proxy",
		Short: "A routing proxy for etcd that directs resource kinds to different etcd backends",
		Long: `etcd-route-proxy sits between kube-apiserver and etcd, routing requests to
different etcd backends based on the Kubernetes resource type (key prefix).

It accepts the same TLS flags as etcd so it can be a drop-in replacement
for the etcd endpoint from kube-apiserver's perspective.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.run()
		},
	}

	// etcd-compatible flags for serving.
	cmd.Flags().StringVar(&opts.listenClientURLs, "listen-client-urls", "https://0.0.0.0:2379",
		"Comma-separated list of URLs to listen on for client traffic (etcd-compatible)")
	cmd.Flags().StringVar(&opts.certFile, "cert-file", "",
		"Path to the TLS certificate for serving (etcd-compatible)")
	cmd.Flags().StringVar(&opts.keyFile, "key-file", "",
		"Path to the TLS key for serving (etcd-compatible)")
	cmd.Flags().StringVar(&opts.trustedCAFile, "trusted-ca-file", "",
		"Path to the CA certificate for verifying client and backend connections (etcd-compatible)")

	// Client TLS for backend connections (when different from serving cert).
	cmd.Flags().StringVar(&opts.clientCertFile, "client-cert-file", "",
		"Path to the TLS certificate for connecting to backend etcd instances (defaults to --cert-file)")
	cmd.Flags().StringVar(&opts.clientKeyFile, "client-key-file", "",
		"Path to the TLS key for connecting to backend etcd instances (defaults to --key-file)")

	// Proxy-specific flags.
	cmd.Flags().StringVar(&opts.routingConfig, "routing-config", "",
		"Path to the routing configuration YAML file (required)")

	// Ignored etcd flags — accepted so etcd command-line arguments can be
	// passed through unchanged, but have no effect on the proxy.
	cmd.Flags().String("listen-peer-urls", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("initial-cluster", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("initial-cluster-state", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("initial-advertise-peer-urls", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("advertise-client-urls", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("peer-cert-file", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("peer-key-file", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("peer-trusted-ca-file", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("name", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("data-dir", "", "Ignored (etcd compatibility)")
	cmd.Flags().String("listen-metrics-urls", "", "Ignored (etcd compatibility)")

	_ = cmd.MarkFlagRequired("routing-config")

	return cmd
}

type options struct {
	listenClientURLs string
	certFile         string
	keyFile          string
	trustedCAFile    string
	clientCertFile   string
	clientKeyFile    string
	routingConfig    string
}

func (o *options) run() error {
	// Set up logger.
	logCfg := zap.NewProductionConfig()
	logCfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	logger, err := logCfg.Build()
	if err != nil {
		return fmt.Errorf("building logger: %w", err)
	}
	defer logger.Sync()

	// Load routing config.
	routingCfg, err := LoadConfig(o.routingConfig)
	if err != nil {
		return fmt.Errorf("loading routing config: %w", err)
	}
	logger.Info("loaded routing config",
		zap.String("defaultEndpoint", routingCfg.DefaultEndpoint),
		zap.Int("routes", len(routingCfg.Routes)),
		zap.String("storagePrefix", routingCfg.StoragePrefix),
	)

	// Parse listen URLs.
	listenURLs := strings.Split(o.listenClientURLs, ",")

	// Resolve client cert/key: default to serving cert if not specified.
	clientCertFile := o.clientCertFile
	if clientCertFile == "" {
		clientCertFile = o.certFile
	}
	clientKeyFile := o.clientKeyFile
	if clientKeyFile == "" {
		clientKeyFile = o.keyFile
	}

	// Create server.
	serverCfg := &ServerConfig{
		ListenClientURLs:  listenURLs,
		CertFile:          o.certFile,
		KeyFile:           o.keyFile,
		TrustedCAFile:     o.trustedCAFile,
		ClientCertFile:    clientCertFile,
		ClientKeyFile:     clientKeyFile,
	}

	srv, err := NewServer(serverCfg, routingCfg, logger)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	// Start listening.
	if err := srv.Listen(listenURLs); err != nil {
		return fmt.Errorf("starting listeners: %w", err)
	}

	// Handle shutdown signals.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
		srv.Stop()
	}()

	logger.Info("etcd-route-proxy is ready")

	// Block on serving.
	err = srv.Serve()
	<-ctx.Done()
	return err
}
