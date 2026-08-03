package etcdrouteproxy

import (
	"fmt"

	component "github.com/openshift/hypershift/support/controlplane-component"
	"github.com/openshift/hypershift/support/podspec"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	routingConfigVolumeName = "routing-config"
	routingConfigMountPath  = "/etc/etcd-route-proxy"
	routingConfigFileName   = "routing.yaml"

	serverTLSMountPath     = "/etc/etcd/tls/server"
	etcdClientTLSMountPath = "/etc/etcd/tls/client"
	etcdCAMountPath        = "/etc/etcd/tls/ca"
)

func adaptDeployment(cpContext component.WorkloadContext, deployment *appsv1.Deployment) error {
	hcp := cpContext.HCP

	routingConfigPath := fmt.Sprintf("%s/%s", routingConfigMountPath, routingConfigFileName)
	// Serving cert: the proxy's own server cert with SANs for etcd-route-proxy service
	servingCertFile := fmt.Sprintf("%s/server.crt", serverTLSMountPath)
	servingKeyFile := fmt.Sprintf("%s/server.key", serverTLSMountPath)
	// Client cert: used to connect to backend etcd instances
	clientCertFile := fmt.Sprintf("%s/etcd-client.crt", etcdClientTLSMountPath)
	clientKeyFile := fmt.Sprintf("%s/etcd-client.key", etcdClientTLSMountPath)
	caFile := fmt.Sprintf("%s/ca.crt", etcdCAMountPath)
	listenURL := "https://0.0.0.0:2379"

	podspec.UpdateContainer(ComponentName, deployment.Spec.Template.Spec.Containers, func(c *corev1.Container) {
		c.Command = []string{"control-plane-operator", "etcd-route-proxy"}
		c.Args = []string{
			"--routing-config", routingConfigPath,
			"--listen-client-urls", listenURL,
			"--cert-file", servingCertFile,
			"--key-file", servingKeyFile,
			"--client-cert-file", clientCertFile,
			"--client-key-file", clientKeyFile,
			"--trusted-ca-file", caFile,
		}
	})

	_ = hcp
	return nil
}
