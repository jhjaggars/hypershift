package pki

import (
	"fmt"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/support/config"

	corev1 "k8s.io/api/core/v1"
)

// Etcd secret keys
const (
	EtcdClientCrtKey = "etcd-client.crt"
	EtcdClientKeyKey = "etcd-client.key"

	EtcdServerCrtKey = "server.crt"
	EtcdServerKeyKey = "server.key"

	EtcdPeerCrtKey = "peer.crt"
	EtcdPeerKeyKey = "peer.key"
)

func ReconcileEtcdClientSecret(secret, ca *corev1.Secret, ownerRef config.OwnerRef) error {
	return reconcileSignedCertWithKeys(secret, ca, ownerRef, "etcd-client", []string{"kubernetes"}, X509UsageClientAuth, EtcdClientCrtKey, EtcdClientKeyKey, "")
}

func ReconcileEtcdMetricsClientSecret(secret, ca *corev1.Secret, ownerRef config.OwnerRef) error {
	return reconcileSignedCertWithKeys(secret, ca, ownerRef, "etcd-metrics-client", []string{"kubernetes"}, X509UsageClientAuth, EtcdClientCrtKey, EtcdClientKeyKey, "")
}

func ReconcileEtcdServerSecret(secret, ca *corev1.Secret, ownerRef config.OwnerRef, hcp *hyperv1.HostedControlPlane) error {
	dnsNames := []string{
		"etcd-client",
		"localhost",
	}

	// Get the list of etcd shards to generate SANs for
	shards := getEtcdShards(hcp)

	// Generate SANs for each etcd shard (works for both single and multiple etcd instances)
	for _, shard := range shards {
		var discoveryName, clientName string
		if len(shards) > 1 {
			// Multi-shard mode: use shard-specific names
			discoveryName = fmt.Sprintf("etcd-%s-discovery", shard.Name)
			clientName = fmt.Sprintf("etcd-%s-client", shard.Name)
		} else {
			// Single etcd mode: use standard names
			discoveryName = "etcd-discovery"
			clientName = "etcd-client"
		}

		dnsNames = append(dnsNames,
			fmt.Sprintf("*.%s.%s.svc", discoveryName, secret.Namespace),
			fmt.Sprintf("*.%s.%s.svc.cluster.local", discoveryName, secret.Namespace),
			fmt.Sprintf("%s.%s.svc", clientName, secret.Namespace),
			fmt.Sprintf("%s.%s.svc.cluster.local", clientName, secret.Namespace),
		)
	}

	return reconcileSignedCertWithKeysAndAddresses(secret, ca, ownerRef, "etcd-server", []string{"kubernetes"}, X509UsageClientServerAuth, EtcdServerCrtKey, EtcdServerKeyKey, "", dnsNames, nil, "")
}

func ReconcileEtcdPeerSecret(secret, ca *corev1.Secret, ownerRef config.OwnerRef, hcp *hyperv1.HostedControlPlane) error {
	dnsNames := []string{
		"127.0.0.1",
		"::1",
	}

	// Get the list of etcd shards to generate SANs for
	shards := getEtcdShards(hcp)

	// Generate SANs for each etcd shard (works for both single and multiple etcd instances)
	for _, shard := range shards {
		var discoveryName string
		if len(shards) > 1 {
			// Multi-shard mode: use shard-specific names
			discoveryName = fmt.Sprintf("etcd-%s-discovery", shard.Name)
		} else {
			// Single etcd mode: use standard names
			discoveryName = "etcd-discovery"
		}

		dnsNames = append(dnsNames,
			fmt.Sprintf("*.%s.%s.svc", discoveryName, secret.Namespace),
			fmt.Sprintf("*.%s.%s.svc.cluster.local", discoveryName, secret.Namespace),
		)
	}

	return reconcileSignedCertWithKeysAndAddresses(secret, ca, ownerRef, "etcd-discovery", []string{"kubernetes"}, X509UsageClientServerAuth, EtcdPeerCrtKey, EtcdPeerKeyKey, "", dnsNames, nil, "")
}

// getEtcdShards returns the list of etcd shards for certificate generation.
// This returns a slice of shard specs whether sharding is enabled or not,
// treating a non-sharded etcd as a single-element collection.
func getEtcdShards(hcp *hyperv1.HostedControlPlane) []hyperv1.EtcdShardSpec {
	if hcp.Spec.Etcd.Managed != nil && hcp.Spec.Etcd.Managed.Sharding != nil && hcp.Spec.Etcd.Managed.Sharding.Enabled {
		return hcp.Spec.Etcd.Managed.Sharding.Shards
	}
	// For non-sharded configuration, return a single default shard
	return []hyperv1.EtcdShardSpec{
		{
			Name: "etcd", // Use "etcd" as the name for backward compatibility
		},
	}
}
