package etcdrouteproxy

import (
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	etcdv2 "github.com/openshift/hypershift/control-plane-operator/controllers/hostedcontrolplane/v2/etcd"
	component "github.com/openshift/hypershift/support/controlplane-component"
	etcdutil "github.com/openshift/hypershift/support/etcd"
)

const (
	ComponentName = "etcd-route-proxy"
)

var _ component.ComponentOptions = &etcdRouteProxy{}

type etcdRouteProxy struct {
}

func (e *etcdRouteProxy) IsRequestServing() bool {
	return false
}

func (e *etcdRouteProxy) MultiZoneSpread() bool {
	return false
}

func (e *etcdRouteProxy) NeedsManagementKASAccess() bool {
	return false
}

func NewComponent() component.ControlPlaneComponent {
	return component.NewDeploymentComponent(ComponentName, &etcdRouteProxy{}).
		WithAdaptFunction(adaptDeployment).
		WithPredicate(isNeeded).
		WithDependencies(etcdv2.ComponentName).
		WithManifestAdapter(
			"routing-config.yaml",
			component.WithAdaptFunction(adaptRoutingConfig),
		).
		Build()
}

// isNeeded returns true when managed etcd has shards that contain at least one
// resource that is NOT an etcd-lease resource (i.e., can be routed via the proxy).
// If every shard resource is an etcd-lease resource (events), the proxy is not needed
// because --etcd-servers-overrides handles those directly.
func isNeeded(cpContext component.WorkloadContext) (bool, error) {
	hcp := cpContext.HCP
	if hcp.Spec.Etcd.ManagementType != hyperv1.Managed {
		return false, nil
	}
	if hcp.Spec.Etcd.Managed == nil || len(hcp.Spec.Etcd.Managed.Shards) == 0 {
		return false, nil
	}

	for _, shard := range hcp.Spec.Etcd.Managed.Shards {
		for _, r := range shard.Resources {
			if !etcdutil.IsETCDLeaseResource(r) {
				return true, nil
			}
		}
	}
	return false, nil
}
