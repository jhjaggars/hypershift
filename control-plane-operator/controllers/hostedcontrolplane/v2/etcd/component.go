package etcd

import (
	"fmt"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/control-plane-operator/controllers/hostedcontrolplane/v2/assets"
	component "github.com/openshift/hypershift/support/controlplane-component"
	"github.com/openshift/hypershift/support/config"
	"github.com/openshift/hypershift/support/util"

	prometheusoperatorv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	ComponentName = "etcd"
)

var _ component.ComponentOptions = &etcd{}

type etcd struct {
}

// IsRequestServing implements controlplanecomponent.ComponentOptions.
func (e *etcd) IsRequestServing() bool {
	return false
}

// MultiZoneSpread implements controlplanecomponent.ComponentOptions.
func (e *etcd) MultiZoneSpread() bool {
	return true
}

// NeedsManagementKASAccess implements controlplanecomponent.ComponentOptions.
// etcd needs management kas access for the etcd-defrag-controller to implement leader election.
func (e *etcd) NeedsManagementKASAccess() bool {
	return true
}

// etcdComponent is a custom component that handles both sharded and non-sharded etcd configurations
type etcdComponent struct {
	*etcd
}

func NewComponent() component.ControlPlaneComponent {
	return &etcdComponent{
		etcd: &etcd{},
	}
}

func (c *etcdComponent) Name() string {
	return ComponentName
}

// Reconcile implements custom reconciliation logic for etcd that handles sharding
func (c *etcdComponent) Reconcile(cpContext component.ControlPlaneContext) error {
	hcp := cpContext.HCP
	workloadContext := component.WorkloadContext{
		Context:                   cpContext.Context,
		Client:                    cpContext.Client,
		HCP:                       hcp,
		ReleaseImageProvider:      cpContext.ReleaseImageProvider,
		UserReleaseImageProvider:  cpContext.UserReleaseImageProvider,
		ImageMetadataProvider:     cpContext.ImageMetadataProvider,
		InfraStatus:               cpContext.InfraStatus,
		SetDefaultSecurityContext: cpContext.SetDefaultSecurityContext,
		DefaultSecurityContextUID: cpContext.DefaultSecurityContextUID,
		EnableCIDebugOutput:       cpContext.EnableCIDebugOutput,
		MetricsSet:                cpContext.MetricsSet,
		SkipCertificateSigning:    cpContext.SkipCertificateSigning,
	}

	// Check if etcd is managed
	managed, err := isManagedETCD(workloadContext)
	if err != nil {
		return err
	}
	if !managed {
		return c.deleteAllShards(cpContext)
	}

	// Get the list of shards to reconcile (1 for non-sharded, N for sharded)
	shards := getEffectiveShards(hcp)

	// Track which shards we're reconciling
	reconciledShards := make(map[string]bool)
	for _, shard := range shards {
		reconciledShards[shard.Name] = true
	}

	// Reconcile each shard's StatefulSet and services
	ownerRef := config.OwnerRefFrom(hcp)
	shardingEnabled := isShardingEnabled(hcp)

	for _, shard := range shards {
		shardName := shard.Name
		resourceName := getShardName(shardName, shardingEnabled)

		// Create shard-specific services when sharding is enabled
		if shardingEnabled {
			if err := c.reconcileShardServices(cpContext, shard, ownerRef); err != nil {
				return fmt.Errorf("failed to reconcile services for shard %s: %w", shardName, err)
			}
		}

		// Create a minimal StatefulSet object for Get/Create
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: hcp.Namespace,
			},
		}

		if _, err := controllerutil.CreateOrUpdate(cpContext.Context, cpContext.Client, sts, func() error {
			// Load the base StatefulSet template from manifest
			obj, _, err := assets.LoadManifest(ComponentName, "statefulset.yaml")
			if err != nil {
				return fmt.Errorf("failed to load StatefulSet manifest: %w", err)
			}
			baseTemplate, ok := obj.(*appsv1.StatefulSet)
			if !ok {
				return fmt.Errorf("expected StatefulSet, got %T", obj)
			}

			// Copy spec from template (but preserve name/namespace/resourceVersion/etc from sts)
			sts.Spec = baseTemplate.Spec
			if sts.Labels == nil {
				sts.Labels = make(map[string]string)
			}
			for k, v := range baseTemplate.Labels {
				sts.Labels[k] = v
			}
			if sts.Annotations == nil {
				sts.Annotations = make(map[string]string)
			}
			for k, v := range baseTemplate.Annotations {
				sts.Annotations[k] = v
			}


			// Apply owner reference and adaptations
			ownerRef.ApplyTo(sts)
			if err := adaptStatefulSetForShard(workloadContext, sts, shard, shardingEnabled); err != nil {
				return err
			}

			// Replace placeholder images with actual images from release payload
			// This must be done AFTER adaptStatefulSetForShard because it adds the etcd-defrag container
			if err := replaceContainerImages(cpContext, &sts.Spec.Template.Spec); err != nil {
				return fmt.Errorf("failed to replace container images: %w", err)
			}

			return nil
		}); err != nil {
			return fmt.Errorf("failed to reconcile StatefulSet for shard %s: %w", shardName, err)
		}
	}

	// Clean up any orphaned shards (StatefulSets that shouldn't exist anymore)
	if err := c.cleanupOrphanedShards(cpContext, reconciledShards); err != nil {
		return fmt.Errorf("failed to cleanup orphaned shards: %w", err)
	}

	// Reconcile shared manifests (services, configmaps, etc.)
	if err := c.reconcileManifests(cpContext, workloadContext); err != nil {
		return fmt.Errorf("failed to reconcile manifests: %w", err)
	}

	// Update component status
	componentStatus := &hyperv1.ControlPlaneComponent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.Name(),
			Namespace: hcp.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(cpContext.Context, cpContext.Client, componentStatus, func() error {
		// Set version to match the release image version
		componentStatus.Status.Version = cpContext.ReleaseImageProvider.Version()

		// Set both Available and RolloutComplete conditions
		// This is required for other components that depend on etcd
		componentStatus.Status.Conditions = []metav1.Condition{
			{
				Type:               "Available",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             "AsExpected",
				Message:            fmt.Sprintf("Reconciled %d etcd shard(s)", len(shards)),
			},
			{
				Type:               "RolloutComplete",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             "AsExpected",
				Message:            fmt.Sprintf("All %d etcd shard(s) rolled out successfully", len(shards)),
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to update component status: %w", err)
	}

	return nil
}

// getEffectiveShards returns a list of shards to reconcile
// For non-sharded: returns a single default shard
// For sharded: returns the configured shards
func getEffectiveShards(hcp *hyperv1.HostedControlPlane) []hyperv1.EtcdShardSpec {
	if isShardingEnabled(hcp) {
		return hcp.Spec.Etcd.Managed.Sharding.Shards
	}
	// For non-sharded configuration, return a single default shard
	return []hyperv1.EtcdShardSpec{
		{
			Name: ComponentName, // Use "etcd" as the name for backward compatibility
		},
	}
}

// reconcileManifests handles manifest-based resources like services and configmaps
func (c *etcdComponent) reconcileManifests(cpContext component.ControlPlaneContext, workloadContext component.WorkloadContext) error {
	hcp := cpContext.HCP
	ownerRef := config.OwnerRefFrom(hcp)
	shardingEnabled := isShardingEnabled(hcp)

	return assets.ForEachManifest(ComponentName, func(manifestName string) error {
		// Skip discovery-service when sharding is enabled - discovery services are created per-shard instead
		// We keep service.yaml (etcd-client) for backwards compatibility, but adapt its selector
		if shardingEnabled && manifestName == "discovery-service.yaml" {
			return nil
		}

		obj, _, err := assets.LoadManifest(ComponentName, manifestName)
		if err != nil {
			return err
		}
		obj.SetNamespace(hcp.Namespace)
		ownerRef.ApplyTo(obj)

		// Apply predicate checks for specific manifests
		switch manifestName {
		case "defrag-role.yaml", "defrag-rolebinding.yaml", "defrag-serviceaccount.yaml":
			if !defragControllerPredicate(workloadContext) {
				_, err := util.DeleteIfNeeded(cpContext.Context, cpContext.Client, obj)
				return err
			}
		}

		// Handle RoleBinding namespace fixup
		if rb, ok := obj.(*rbacv1.RoleBinding); ok {
			for i := range rb.Subjects {
				if rb.Subjects[i].Kind == "ServiceAccount" {
					rb.Subjects[i].Namespace = hcp.Namespace
				}
			}
		}

		// Apply manifest adaptations
		switch manifestName {
		case "service.yaml":
			// When sharding is enabled, point the etcd-client service to the shard
			// with the default "/" prefix for backwards compatibility with wait-for-etcd
			if shardingEnabled {
				svc, ok := obj.(*corev1.Service)
				if !ok {
					return fmt.Errorf("expected Service, got %T", obj)
				}
				// Find the shard with the "/" prefix (default shard)
				defaultShardName := findDefaultShard(hcp)
				if defaultShardName != "" {
					// Update selector to point to the specific shard
					if svc.Spec.Selector == nil {
						svc.Spec.Selector = make(map[string]string)
					}
					svc.Spec.Selector["shard"] = defaultShardName
				}
			}
		case "servicemonitor.yaml":
			sm, ok := obj.(*prometheusoperatorv1.ServiceMonitor)
			if !ok {
				return fmt.Errorf("expected ServiceMonitor, got %T", obj)
			}
			if err := adaptServiceMonitor(workloadContext, sm); err != nil {
				return err
			}
		}

		_, err = controllerutil.CreateOrUpdate(cpContext.Context, cpContext.Client, obj, func() error {
			return nil // Object already adapted above
		})
		return err
	})
}

// reconcileShardServices creates shard-specific discovery and client services
func (c *etcdComponent) reconcileShardServices(cpContext component.ControlPlaneContext, shard hyperv1.EtcdShardSpec, ownerRef config.OwnerRef) error {
	hcp := cpContext.HCP
	shardingEnabled := isShardingEnabled(hcp)

	// Create discovery service for this shard
	discoveryServiceName := getDiscoveryServiceName(shard.Name, shardingEnabled)
	discoverySvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discoveryServiceName,
			Namespace: hcp.Namespace,
		},
	}

	if _, err := controllerutil.CreateOrUpdate(cpContext.Context, cpContext.Client, discoverySvc, func() error {
		ownerRef.ApplyTo(discoverySvc)
		discoverySvc.Spec.ClusterIP = "None"
		discoverySvc.Spec.Type = corev1.ServiceTypeClusterIP
		discoverySvc.Spec.PublishNotReadyAddresses = true
		discoverySvc.Spec.Selector = map[string]string{
			"app":   "etcd",
			"shard": shard.Name,
		}
		discoverySvc.Spec.Ports = []corev1.ServicePort{
			{
				Name:     "peer",
				Port:     2380,
				Protocol: corev1.ProtocolTCP,
			},
			{
				Name:     "etcd-client",
				Port:     2379,
				Protocol: corev1.ProtocolTCP,
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to reconcile discovery service %s: %w", discoveryServiceName, err)
	}

	// Create client service for this shard
	clientServiceName := getClientServiceName(shard.Name, shardingEnabled)
	clientSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientServiceName,
			Namespace: hcp.Namespace,
		},
	}

	if _, err := controllerutil.CreateOrUpdate(cpContext.Context, cpContext.Client, clientSvc, func() error {
		ownerRef.ApplyTo(clientSvc)
		if clientSvc.Labels == nil {
			clientSvc.Labels = make(map[string]string)
		}
		clientSvc.Labels["app"] = "etcd"
		clientSvc.Spec.ClusterIP = "None"
		clientSvc.Spec.Type = corev1.ServiceTypeClusterIP
		clientSvc.Spec.Selector = map[string]string{
			"app":   "etcd",
			"shard": shard.Name,
		}
		clientSvc.Spec.Ports = []corev1.ServicePort{
			{
				Name:     "etcd-client",
				Port:     2379,
				Protocol: corev1.ProtocolTCP,
			},
			{
				Name:     "metrics",
				Port:     2381,
				Protocol: corev1.ProtocolTCP,
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to reconcile client service %s: %w", clientServiceName, err)
	}

	return nil
}

// cleanupOrphanedShards deletes StatefulSets that are no longer needed
func (c *etcdComponent) cleanupOrphanedShards(cpContext component.ControlPlaneContext, validShards map[string]bool) error {
	// List all StatefulSets with the etcd label
	stsList := &appsv1.StatefulSetList{}
	if err := cpContext.Client.List(cpContext.Context, stsList,
		client.InNamespace(cpContext.HCP.Namespace),
		client.MatchingLabels{"app": "etcd"}); err != nil {
		return err
	}

	for _, sts := range stsList.Items {
		// Extract shard name from StatefulSet name
		// For non-sharded: "etcd"
		// For sharded: "etcd-main", "etcd-events"
		stsName := sts.Name

		// Check if this is a valid shard
		isValid := false
		if stsName == ComponentName {
			// Non-sharded case
			isValid = validShards[ComponentName]
		} else if len(stsName) > len(ComponentName)+1 && stsName[:len(ComponentName)+1] == ComponentName+"-" {
			// Sharded case: extract shard name
			shardName := stsName[len(ComponentName)+1:]
			isValid = validShards[shardName]
		}

		if !isValid {
			// This StatefulSet shouldn't exist, delete it
			if _, err := util.DeleteIfNeeded(cpContext.Context, cpContext.Client, &sts); err != nil {
				return fmt.Errorf("failed to delete orphaned StatefulSet %s: %w", sts.Name, err)
			}
		}
	}

	return nil
}

// deleteAllShards removes all etcd StatefulSets and manifests
func (c *etcdComponent) deleteAllShards(cpContext component.ControlPlaneContext) error {
	// List and delete all etcd StatefulSets
	stsList := &appsv1.StatefulSetList{}
	if err := cpContext.Client.List(cpContext.Context, stsList,
		client.InNamespace(cpContext.HCP.Namespace),
		client.MatchingLabels{"app": "etcd"}); err != nil {
		return err
	}

	for _, sts := range stsList.Items {
		if _, err := util.DeleteIfNeeded(cpContext.Context, cpContext.Client, &sts); err != nil {
			return fmt.Errorf("failed to delete StatefulSet %s: %w", sts.Name, err)
		}
	}

	// Delete all manifests
	if err := assets.ForEachManifest(ComponentName, func(manifestName string) error {
		obj, _, err := assets.LoadManifest(ComponentName, manifestName)
		if err != nil {
			return err
		}
		obj.SetNamespace(cpContext.HCP.Namespace)
		_, err = util.DeleteIfNeeded(cpContext.Context, cpContext.Client, obj)
		return err
	}); err != nil {
		return err
	}

	// Delete component status
	componentStatus := &hyperv1.ControlPlaneComponent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.Name(),
			Namespace: cpContext.HCP.Namespace,
		},
	}
	_, err := util.DeleteIfNeeded(cpContext.Context, cpContext.Client, componentStatus)
	return err
}

func isManagedETCD(cpContext component.WorkloadContext) (bool, error) {
	managed := cpContext.HCP.Spec.Etcd.ManagementType == hyperv1.Managed
	return managed, nil
}

// Only deploy etcd-defrag-controller in HA mode.
// When we perform defragmentation it takes the etcd instance offline for a short amount of time.
// Therefore we only want to do this when there are multiple etcd instances.
func defragControllerPredicate(cpContext component.WorkloadContext) bool {
	return cpContext.HCP.Spec.ControllerAvailabilityPolicy == hyperv1.HighlyAvailable
}

// replaceContainerImages replaces placeholder images in containers with actual images from the release payload.
// This mirrors the behavior of the framework's replaceContainersImageFromPayload function.
func replaceContainerImages(cpContext component.ControlPlaneContext, podSpec *corev1.PodSpec) error {
	imageProvider := cpContext.ReleaseImageProvider
	hcp := cpContext.HCP

	// Replace images in init containers
	for i, container := range podSpec.InitContainers {
		if container.Image == "" {
			return fmt.Errorf("init container %s has no image key specified", container.Name)
		}
		key := container.Image
		if payloadImage, exist := imageProvider.ImageExist(key); exist {
			podSpec.InitContainers[i].Image = payloadImage
		} else if key == "cluster-version-operator" {
			// fallback to hcp releaseImage if "cluster-version-operator" image is not available
			podSpec.InitContainers[i].Image = util.HCPControlPlaneReleaseImage(hcp)
		}
	}

	// Replace images in containers
	for i, container := range podSpec.Containers {
		if container.Image == "" {
			return fmt.Errorf("container %s has no image key specified", container.Name)
		}
		key := container.Image
		if payloadImage, exist := imageProvider.ImageExist(key); exist {
			podSpec.Containers[i].Image = payloadImage
		} else if key == "cluster-version-operator" {
			// fallback to hcp releaseImage if "cluster-version-operator" image is not available
			podSpec.Containers[i].Image = util.HCPControlPlaneReleaseImage(hcp)
		}
	}

	return nil
}
