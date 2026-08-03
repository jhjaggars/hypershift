package etcdrouteproxy

import (
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// Config defines the routing configuration for the etcd route proxy.
type Config struct {
	// StoragePrefix is the etcd key prefix used by kube-apiserver.
	// Defaults to "/registry" if not set.
	StoragePrefix string `json:"storagePrefix,omitempty"`

	// DefaultEndpoint is the etcd endpoint for resources not matched by any route.
	DefaultEndpoint string `json:"defaultEndpoint"`

	// Routes maps symbolic resource names to etcd endpoints.
	Routes []Route `json:"routes,omitempty"`
}

// Route defines a named set of resources that should be stored in a specific etcd endpoint.
type Route struct {
	// Name is a symbolic name for this route (for logging/metrics).
	Name string `json:"name"`

	// Resources lists the Kubernetes resource types routed to this endpoint.
	Resources []RouteResource `json:"resources"`

	// Endpoint is the etcd endpoint URL for this route.
	Endpoint string `json:"endpoint"`
}

// RouteResource identifies a Kubernetes resource type, matching the EtcdShardResource
// shape from the HostedCluster API.
type RouteResource struct {
	// APIGroup is the API group of the resource (empty string for core group).
	APIGroup string `json:"apiGroup"`

	// Resource is the plural resource name (e.g., "events", "leases").
	Resource string `json:"resource"`
}

// routeTable is the resolved routing table used at runtime.
type routeTable struct {
	storagePrefix  string
	defaultBackend string
	// prefixToBackend maps etcd key prefixes to backend endpoint URLs.
	// Keys are full prefixes like "/registry/events/" or "/registry/coordination.k8s.io/leases/".
	prefixToBackend map[string]string
}

// LoadConfig reads and parses a routing config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	if cfg.DefaultEndpoint == "" {
		return nil, fmt.Errorf("defaultEndpoint is required")
	}
	if cfg.StoragePrefix == "" {
		cfg.StoragePrefix = "/registry"
	}
	// Normalize: ensure prefix has leading slash and no trailing slash.
	cfg.StoragePrefix = "/" + strings.Trim(cfg.StoragePrefix, "/")
	return &cfg, nil
}

// buildRouteTable resolves a Config into a routeTable for fast prefix matching.
func buildRouteTable(cfg *Config) (*routeTable, error) {
	rt := &routeTable{
		storagePrefix:   cfg.StoragePrefix,
		defaultBackend:  cfg.DefaultEndpoint,
		prefixToBackend: make(map[string]string),
	}

	for _, route := range cfg.Routes {
		if route.Endpoint == "" {
			return nil, fmt.Errorf("route %q: endpoint is required", route.Name)
		}
		for _, r := range route.Resources {
			prefix := resourceToPrefix(cfg.StoragePrefix, r)
			if _, exists := rt.prefixToBackend[prefix]; exists {
				return nil, fmt.Errorf("route %q: duplicate prefix %q", route.Name, prefix)
			}
			rt.prefixToBackend[prefix] = route.Endpoint
		}
	}

	return rt, nil
}

// resourceToPrefix converts a RouteResource to the etcd key prefix that
// kube-apiserver uses. The format is:
//
//	Core group:    /registry/<resource>/
//	Named group:   /registry/<apiGroup>/<resource>/
func resourceToPrefix(storagePrefix string, r RouteResource) string {
	if r.APIGroup == "" {
		return storagePrefix + "/" + r.Resource + "/"
	}
	return storagePrefix + "/" + r.APIGroup + "/" + r.Resource + "/"
}
