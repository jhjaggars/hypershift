package etcd

import (
	_ "embed"
	"fmt"
	"strings"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/control-plane-operator/controllers/hostedcontrolplane/manifests"
	component "github.com/openshift/hypershift/support/controlplane-component"
	"github.com/openshift/hypershift/support/util"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
)

func adaptStatefulSet(cpContext component.WorkloadContext, sts *appsv1.StatefulSet) error {
	// For non-sharded (backward compatibility), create a default shard with no replica override
	defaultShard := hyperv1.EtcdShardSpec{
		Name: ComponentName,
	}
	return adaptStatefulSetForShard(cpContext, sts, defaultShard, false)
}

// adaptStatefulSetForShard adapts a StatefulSet for a specific etcd shard.
// shard contains the shard specification including name and replica count.
// shardingEnabled indicates whether multi-shard mode is active.
func adaptStatefulSetForShard(cpContext component.WorkloadContext, sts *appsv1.StatefulSet, shard hyperv1.EtcdShardSpec, shardingEnabled bool) error {
	hcp := cpContext.HCP
	managedEtcdSpec := hcp.Spec.Etcd.Managed

	// Get the discovery service name for this shard
	discoveryServiceName := getDiscoveryServiceName(shard.Name, shardingEnabled)

	// Update StatefulSet name and serviceName for sharded deployments
	if shardingEnabled {
		sts.Name = formatShardName(shard.Name)
		// Update serviceName to use shard-specific discovery service
		sts.Spec.ServiceName = discoveryServiceName
		// Add shard label to pod template for service selection
		if sts.Spec.Template.Labels == nil {
			sts.Spec.Template.Labels = make(map[string]string)
		}
		sts.Spec.Template.Labels["shard"] = shard.Name
	}

	// Set replica count from shard spec if specified, otherwise use default
	if shard.Replicas != nil && *shard.Replicas > 0 {
		sts.Spec.Replicas = shard.Replicas
	} else {
		// Fall back to default replica calculation
		replicas := component.DefaultReplicas(hcp, &etcd{}, ComponentName)
		sts.Spec.Replicas = &replicas
	}

	ipv4, err := util.IsIPv4CIDR(hcp.Spec.Networking.ClusterNetwork[0].CIDR.String())
	if err != nil {
		return fmt.Errorf("error checking the ClusterNetworkCIDR: %v", err)
	}

	// Determine the effective replica count for this shard
	var effectiveReplicas int32
	if sts.Spec.Replicas != nil {
		effectiveReplicas = *sts.Spec.Replicas
	} else {
		effectiveReplicas = component.DefaultReplicas(hcp, &etcd{}, ComponentName)
	}

	util.UpdateContainer(ComponentName, sts.Spec.Template.Spec.Containers, func(c *corev1.Container) {
		var members []string
		for i := int32(0); i < effectiveReplicas; i++ {
			// Pod names within a shard: etcd-{shardName}-0, etcd-{shardName}-1, etc. (or etcd-0, etcd-1 for single shard)
			podName := fmt.Sprintf("%s-%d", getShardName(shard.Name, shardingEnabled), i)
			members = append(members, fmt.Sprintf("%s=https://%s.%s.%s.svc:2380", podName, podName, discoveryServiceName, hcp.Namespace))
		}
		c.Env = append(c.Env,
			corev1.EnvVar{
				Name:  "ETCD_INITIAL_CLUSTER",
				Value: strings.Join(members, ","),
			},
		)

		// Update advertise URLs to use shard-specific discovery service
		util.UpsertEnvVar(c, corev1.EnvVar{
			Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
			Value: fmt.Sprintf("https://$(HOSTNAME).%s.$(NAMESPACE).svc:2380", discoveryServiceName),
		})
		util.UpsertEnvVar(c, corev1.EnvVar{
			Name:  "ETCD_ADVERTISE_CLIENT_URLS",
			Value: fmt.Sprintf("https://$(HOSTNAME).%s.$(NAMESPACE).svc:2379", discoveryServiceName),
		})

		if !ipv4 {
			util.UpsertEnvVar(c, corev1.EnvVar{
				Name:  "ETCD_LISTEN_PEER_URLS",
				Value: "https://[$(POD_IP)]:2380",
			})
			util.UpsertEnvVar(c, corev1.EnvVar{
				Name:  "ETCD_LISTEN_CLIENT_URLS",
				Value: "https://[$(POD_IP)]:2379,https://localhost:2379",
			})
			util.UpsertEnvVar(c, corev1.EnvVar{
				Name:  "ETCD_LISTEN_METRICS_URLS",
				Value: "https://[::]:2382",
			})
		}
	})

	util.UpdateContainer("etcd-metrics", sts.Spec.Template.Spec.Containers, func(c *corev1.Container) {
		var loInterface, allInterfaces string
		if ipv4 {
			loInterface = "127.0.0.1"
			allInterfaces = "0.0.0.0"
		} else {
			loInterface = "[::1]"
			allInterfaces = "[::]"
		}
		c.Args = append(c.Args,
			fmt.Sprintf("--listen-addr=%s:2383", loInterface),
			fmt.Sprintf("--metrics-addr=https://%s:2381", allInterfaces),
		)
	})

	if defragControllerPredicate(cpContext) {
		// Only add defrag container if it doesn't already exist
		hasDefrag := false
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name == "etcd-defrag" {
				hasDefrag = true
				break
			}
		}
		if !hasDefrag {
			sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, buildEtcdDefragControllerContainer(hcp.Namespace))
		}
		sts.Spec.Template.Spec.ServiceAccountName = manifests.EtcdDefragControllerServiceAccount("").Name
	}

	snapshotRestored := meta.IsStatusConditionTrue(hcp.Status.Conditions, string(hyperv1.EtcdSnapshotRestored))
	if managedEtcdSpec != nil && len(managedEtcdSpec.Storage.RestoreSnapshotURL) > 0 && !snapshotRestored {
		sts.Spec.Template.Spec.InitContainers = append(sts.Spec.Template.Spec.InitContainers,
			buildEtcdInitContainer(managedEtcdSpec.Storage.RestoreSnapshotURL[0]), // RestoreSnapshotURL can only have 1 entry
		)
	}

	// adapt PersistentVolume
	if managedEtcdSpec != nil && managedEtcdSpec.Storage.Type == hyperv1.PersistentVolumeEtcdStorage {
		if pv := managedEtcdSpec.Storage.PersistentVolume; pv != nil {
			sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName = pv.StorageClassName
			if pv.Size != nil {
				sts.Spec.VolumeClaimTemplates[0].Spec.Resources = corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *pv.Size,
					},
				}
			}
		}
	}

	// Add Pod Security admission compliance for restricted policy
	// Set pod-level security context
	runAsNonRoot := true
	runAsUser := int64(1000) // Use a non-root user ID
	sts.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsNonRoot: &runAsNonRoot,
		RunAsUser:    &runAsUser,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}

	// Apply container-level security contexts to all init containers
	allowPrivilegeEscalation := false
	for i := range sts.Spec.Template.Spec.InitContainers {
		if sts.Spec.Template.Spec.InitContainers[i].SecurityContext == nil {
			sts.Spec.Template.Spec.InitContainers[i].SecurityContext = &corev1.SecurityContext{}
		}
		sts.Spec.Template.Spec.InitContainers[i].SecurityContext.AllowPrivilegeEscalation = &allowPrivilegeEscalation
		sts.Spec.Template.Spec.InitContainers[i].SecurityContext.Capabilities = &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		}
		sts.Spec.Template.Spec.InitContainers[i].SecurityContext.RunAsNonRoot = &runAsNonRoot
	}

	// Apply container-level security contexts to all containers
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].SecurityContext == nil {
			sts.Spec.Template.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{}
		}
		sts.Spec.Template.Spec.Containers[i].SecurityContext.AllowPrivilegeEscalation = &allowPrivilegeEscalation
		sts.Spec.Template.Spec.Containers[i].SecurityContext.Capabilities = &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		}
		sts.Spec.Template.Spec.Containers[i].SecurityContext.RunAsNonRoot = &runAsNonRoot
	}

	// Update init containers to use shard-specific discovery service
	for i := range sts.Spec.Template.Spec.InitContainers {
		initContainer := &sts.Spec.Template.Spec.InitContainers[i]

		// Update ensure-dns init container
		if initContainer.Name == "ensure-dns" {
			for j := range initContainer.Args {
				// Replace "etcd-discovery" with shard-specific discovery service name
				initContainer.Args[j] = strings.ReplaceAll(
					initContainer.Args[j],
					"${HOSTNAME}.etcd-discovery.${NAMESPACE}.svc",
					fmt.Sprintf("${HOSTNAME}.%s.${NAMESPACE}.svc", discoveryServiceName),
				)
			}
		}

		// Update reset-member init container
		if initContainer.Name == "reset-member" {
			for j := range initContainer.Args {
				// Replace "etcd-discovery" with shard-specific discovery service name
				initContainer.Args[j] = strings.ReplaceAll(
					initContainer.Args[j],
					".etcd-discovery.",
					fmt.Sprintf(".%s.", discoveryServiceName),
				)
			}
		}
	}

	return nil
}

//go:embed etcd-init.sh
var etcdInitScript string

func buildEtcdInitContainer(restoreUrl string) corev1.Container {
	c := corev1.Container{
		Name: "etcd-init",
	}
	c.Env = []corev1.EnvVar{
		{
			Name:  "RESTORE_URL_ETCD",
			Value: restoreUrl,
		},
	}
	c.Image = "etcd"
	c.ImagePullPolicy = corev1.PullIfNotPresent
	c.Command = []string{"/bin/sh", "-ce", etcdInitScript}
	c.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "data",
			MountPath: "/var/lib",
		},
	}
	return c
}

func buildEtcdDefragControllerContainer(namespace string) corev1.Container {
	c := corev1.Container{
		Name: "etcd-defrag",
	}
	c.Image = "controlplane-operator"
	c.ImagePullPolicy = corev1.PullIfNotPresent
	c.Command = []string{"control-plane-operator"}
	c.Args = []string{
		"etcd-defrag-controller",
		"--namespace",
		namespace,
	}
	c.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "client-tls",
			MountPath: "/etc/etcd/tls/client",
		},
		{
			Name:      "etcd-ca",
			MountPath: "/etc/etcd/tls/etcd-ca",
		},
	}
	c.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("50Mi"),
		},
	}
	return c
}
