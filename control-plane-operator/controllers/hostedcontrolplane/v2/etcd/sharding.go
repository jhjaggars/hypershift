package etcd

import (
	"fmt"
	"strings"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
)

// isShardingEnabled returns true if etcd sharding is configured for this hosted cluster.
func isShardingEnabled(hcp *hyperv1.HostedControlPlane) bool {
	return hcp.Spec.Etcd.ManagementType == hyperv1.Managed &&
		hcp.Spec.Etcd.Managed != nil &&
		hcp.Spec.Etcd.Managed.Sharding != nil &&
		hcp.Spec.Etcd.Managed.Sharding.Enabled
}

// getShards returns the list of etcd shards configured for this hosted cluster.
// Returns an empty slice if sharding is not enabled.
func getShards(hcp *hyperv1.HostedControlPlane) []hyperv1.EtcdShardSpec {
	if isShardingEnabled(hcp) {
		return hcp.Spec.Etcd.Managed.Sharding.Shards
	}
	return nil
}

// getShardName returns the resource name for an etcd shard.
// When sharding is disabled, returns "etcd" for backward compatibility.
// When sharding is enabled, returns "etcd-{shardName}" where shardName comes from the shard spec.
func getShardName(shardName string, shardingEnabled bool) string {
	if !shardingEnabled {
		return ComponentName
	}
	return formatShardName(shardName)
}

// formatShardName formats the shard name as "etcd-{name}".
func formatShardName(shardName string) string {
	return fmt.Sprintf("etcd-%s", shardName)
}

// getDiscoveryServiceName returns the discovery service name for an etcd shard.
func getDiscoveryServiceName(shardName string, shardingEnabled bool) string {
	if !shardingEnabled {
		return "etcd-discovery"
	}
	return fmt.Sprintf("etcd-%s-discovery", shardName)
}

// getClientServiceName returns the client service name for an etcd shard.
func getClientServiceName(shardName string, shardingEnabled bool) string {
	if !shardingEnabled {
		return "etcd-client"
	}
	return fmt.Sprintf("etcd-%s-client", shardName)
}

// getEtcdClientURLs returns a comma-separated list of etcd client URLs for all shards.
// This is used to configure the kube-apiserver with all etcd endpoints.
func getEtcdClientURLs(hcp *hyperv1.HostedControlPlane) string {
	shards := getShards(hcp)
	shardingEnabled := isShardingEnabled(hcp)

	var urls []string
	if shardingEnabled {
		for _, shard := range shards {
			serviceName := getClientServiceName(shard.Name, shardingEnabled)
			url := fmt.Sprintf("https://%s.%s.svc:2379", serviceName, hcp.Namespace)
			urls = append(urls, url)
		}
	} else {
		// Single etcd (backward compatible)
		serviceName := getClientServiceName("", false)
		url := fmt.Sprintf("https://%s.%s.svc:2379", serviceName, hcp.Namespace)
		urls = append(urls, url)
	}

	return strings.Join(urls, ",")
}
