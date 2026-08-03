package etcdrouteproxy

import (
	"fmt"

	component "github.com/openshift/hypershift/support/controlplane-component"
	etcdutil "github.com/openshift/hypershift/support/etcd"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	etcdrouteproxy "github.com/openshift/hypershift/etcd-route-proxy"
)

const (
	routingConfigMapName = "etcd-route-proxy-config"
)

func adaptRoutingConfig(cpContext component.WorkloadContext, cm *corev1.ConfigMap) error {
	hcp := cpContext.HCP

	cfg := etcdrouteproxy.Config{
		DefaultEndpoint: fmt.Sprintf("https://etcd-client.%s.svc:2379", hcp.Namespace),
	}

	if hcp.Spec.Etcd.Managed != nil {
		for _, shard := range hcp.Spec.Etcd.Managed.Shards {
			var proxyResources []etcdrouteproxy.RouteResource
			for _, r := range shard.Resources {
				if etcdutil.IsETCDLeaseResource(r) {
					// TTL-bearing resources are routed via --etcd-servers-overrides,
					// not through the proxy.
					continue
				}
				group := ""
				if r.APIGroup != nil {
					group = *r.APIGroup
				}
				proxyResources = append(proxyResources, etcdrouteproxy.RouteResource{
					APIGroup: group,
					Resource: r.Resource,
				})
			}
			if len(proxyResources) == 0 {
				continue
			}

			shardEndpoint := fmt.Sprintf("https://%s.%s.svc:2379",
				etcdutil.ClientServiceName(fmt.Sprintf("etcd-%s", shard.Name)),
				hcp.Namespace)

			cfg.Routes = append(cfg.Routes, etcdrouteproxy.Route{
				Name:      shard.Name,
				Resources: proxyResources,
				Endpoint:  shardEndpoint,
			})
		}
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling routing config: %w", err)
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[routingConfigFileName] = string(data)
	return nil
}
