package etcdrouteproxy

import (
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRoute(t *testing.T) {
	tests := []struct {
		name       string
		defaultEP  string
		routes     map[string]string
		key        string
		wantTarget string
	}{
		{
			name:      "When key matches a core-group resource prefix, it should route to that backend",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "/registry/events/default/my-event",
			wantTarget: "https://etcd-events:2379",
		},
		{
			name:      "When key matches a named-group resource prefix, it should route to that backend",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/coordination.k8s.io/leases/": "https://etcd-leases:2379",
			},
			key:        "/registry/coordination.k8s.io/leases/kube-system/kube-controller-manager",
			wantTarget: "https://etcd-leases:2379",
		},
		{
			name:      "When key does not match any route, it should route to the default backend",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "/registry/pods/default/my-pod",
			wantTarget: "https://etcd-default:2379",
		},
		{
			name:      "When key is the prefix itself, it should route to that backend",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "/registry/events/",
			wantTarget: "https://etcd-events:2379",
		},
		{
			name:      "When key is a partial prefix match, it should route to default",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "/registry/event",
			wantTarget: "https://etcd-default:2379",
		},
		{
			name:      "When multiple routes exist, it should match the correct one",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/":                     "https://etcd-events:2379",
				"/registry/coordination.k8s.io/leases/": "https://etcd-leases:2379",
			},
			key:        "/registry/coordination.k8s.io/leases/kube-node-lease/worker-1",
			wantTarget: "https://etcd-leases:2379",
		},
		{
			name:       "When no routes are configured, it should route everything to default",
			defaultEP:  "https://etcd-default:2379",
			routes:     map[string]string{},
			key:        "/registry/configmaps/kube-system/my-config",
			wantTarget: "https://etcd-default:2379",
		},
		{
			name:      "When key is empty, it should route to default",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "",
			wantTarget: "https://etcd-default:2379",
		},
		{
			name:      "When multiple resources share the same backend, it should route each correctly",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/":                     "https://etcd-shard:2379",
				"/registry/coordination.k8s.io/leases/": "https://etcd-shard:2379",
			},
			key:        "/registry/events/default/event-123",
			wantTarget: "https://etcd-shard:2379",
		},
		{
			name:      "When key has the prefix as a substring but not at the start, it should route to default",
			defaultEP: "https://etcd-default:2379",
			routes: map[string]string{
				"/registry/events/": "https://etcd-events:2379",
			},
			key:        "/other/registry/events/default/my-event",
			wantTarget: "https://etcd-default:2379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := &Router{
				table: &routeTable{
					defaultBackend:  tt.defaultEP,
					prefixToBackend: tt.routes,
				},
			}

			got := router.routeToEndpoint([]byte(tt.key))
			if got != tt.wantTarget {
				t.Errorf("routeToEndpoint(%q) = %q, want %q", tt.key, got, tt.wantTarget)
			}
		})
	}
}

func TestDefaultClient(t *testing.T) {
	t.Run("When called, it should return the client for the default backend endpoint", func(t *testing.T) {
		defaultClient := &clientv3.Client{}
		router := &Router{
			table: &routeTable{
				defaultBackend:  "https://etcd-default:2379",
				prefixToBackend: map[string]string{},
			},
			clients: map[string]*clientv3.Client{
				"https://etcd-default:2379": defaultClient,
			},
		}
		got := router.DefaultClient()
		if got != defaultClient {
			t.Error("DefaultClient() did not return the expected client")
		}
	})
}

func TestAllClients(t *testing.T) {
	t.Run("When multiple routes point to the same endpoint, it should deduplicate clients", func(t *testing.T) {
		sharedClient := &clientv3.Client{}
		defaultClient := &clientv3.Client{}
		router := &Router{
			table: &routeTable{
				defaultBackend: "https://etcd-default:2379",
				prefixToBackend: map[string]string{
					"/registry/events/":                     "https://etcd-shard:2379",
					"/registry/coordination.k8s.io/leases/": "https://etcd-shard:2379",
				},
			},
			clients: map[string]*clientv3.Client{
				"https://etcd-default:2379": defaultClient,
				"https://etcd-shard:2379":   sharedClient,
			},
		}
		all := router.AllClients()
		if len(all) != 2 {
			t.Errorf("expected 2 unique clients, got %d", len(all))
		}
	})

	t.Run("When only the default backend exists, it should return one client", func(t *testing.T) {
		defaultClient := &clientv3.Client{}
		router := &Router{
			table: &routeTable{
				defaultBackend:  "https://etcd-default:2379",
				prefixToBackend: map[string]string{},
			},
			clients: map[string]*clientv3.Client{
				"https://etcd-default:2379": defaultClient,
			},
		}
		all := router.AllClients()
		if len(all) != 1 {
			t.Errorf("expected 1 client, got %d", len(all))
		}
		if all[0] != defaultClient {
			t.Error("expected the default client")
		}
	})
}
