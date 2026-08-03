package etcdrouteproxy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		wantErr   bool
		checkFunc func(t *testing.T, cfg *Config)
	}{
		{
			name: "valid config with default storage prefix",
			content: `
defaultEndpoint: https://etcd-client:2379
routes:
  - name: events
    resources:
      - apiGroup: ""
        resource: events
      - apiGroup: coordination.k8s.io
        resource: leases
    endpoint: https://etcd-client-events:2379
`,
			checkFunc: func(t *testing.T, cfg *Config) {
				if cfg.DefaultEndpoint != "https://etcd-client:2379" {
					t.Errorf("unexpected defaultEndpoint: %s", cfg.DefaultEndpoint)
				}
				if cfg.StoragePrefix != "/registry" {
					t.Errorf("unexpected storagePrefix: %s", cfg.StoragePrefix)
				}
				if len(cfg.Routes) != 1 {
					t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
				}
				if len(cfg.Routes[0].Resources) != 2 {
					t.Fatalf("expected 2 resources, got %d", len(cfg.Routes[0].Resources))
				}
			},
		},
		{
			name: "custom storage prefix",
			content: `
storagePrefix: /custom-prefix
defaultEndpoint: https://etcd:2379
`,
			checkFunc: func(t *testing.T, cfg *Config) {
				if cfg.StoragePrefix != "/custom-prefix" {
					t.Errorf("unexpected storagePrefix: %s", cfg.StoragePrefix)
				}
			},
		},
		{
			name:    "missing default endpoint",
			content: `routes: []`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(tt.content), 0644); err != nil {
				t.Fatal(err)
			}

			cfg, err := LoadConfig(path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, cfg)
			}
		})
	}
}

func TestBuildRouteTable(t *testing.T) {
	cfg := &Config{
		StoragePrefix:   "/registry",
		DefaultEndpoint: "https://etcd-default:2379",
		Routes: []Route{
			{
				Name: "events",
				Resources: []RouteResource{
					{APIGroup: "", Resource: "events"},
				},
				Endpoint: "https://etcd-events:2379",
			},
			{
				Name: "leases",
				Resources: []RouteResource{
					{APIGroup: "coordination.k8s.io", Resource: "leases"},
				},
				Endpoint: "https://etcd-leases:2379",
			},
		},
	}

	rt, err := buildRouteTable(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := map[string]string{
		"/registry/events/":                        "https://etcd-events:2379",
		"/registry/coordination.k8s.io/leases/":    "https://etcd-leases:2379",
	}

	if len(rt.prefixToBackend) != len(expected) {
		t.Fatalf("expected %d prefixes, got %d", len(expected), len(rt.prefixToBackend))
	}

	for prefix, wantEP := range expected {
		gotEP, ok := rt.prefixToBackend[prefix]
		if !ok {
			t.Errorf("missing prefix %q", prefix)
			continue
		}
		if gotEP != wantEP {
			t.Errorf("prefix %q: got %q, want %q", prefix, gotEP, wantEP)
		}
	}
}

func TestBuildRouteTableDuplicatePrefix(t *testing.T) {
	cfg := &Config{
		StoragePrefix:   "/registry",
		DefaultEndpoint: "https://etcd:2379",
		Routes: []Route{
			{
				Name:      "r1",
				Resources: []RouteResource{{APIGroup: "", Resource: "events"}},
				Endpoint:  "https://etcd-1:2379",
			},
			{
				Name:      "r2",
				Resources: []RouteResource{{APIGroup: "", Resource: "events"}},
				Endpoint:  "https://etcd-2:2379",
			},
		},
	}

	_, err := buildRouteTable(cfg)
	if err == nil {
		t.Fatal("expected error for duplicate prefix")
	}
}

func TestResourceToPrefix(t *testing.T) {
	tests := []struct {
		name     string
		resource RouteResource
		want     string
	}{
		{
			name:     "core group resource",
			resource: RouteResource{APIGroup: "", Resource: "events"},
			want:     "/registry/events/",
		},
		{
			name:     "named group resource",
			resource: RouteResource{APIGroup: "coordination.k8s.io", Resource: "leases"},
			want:     "/registry/coordination.k8s.io/leases/",
		},
		{
			name:     "apps group",
			resource: RouteResource{APIGroup: "apps", Resource: "deployments"},
			want:     "/registry/apps/deployments/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resourceToPrefix("/registry", tt.resource)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
