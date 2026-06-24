package provider

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/zgfh/k8s-remote-node/remote"
)

// SSHNodeProvider implements node.NodeProvider for heartbeat and node status updates.
type SSHNodeProvider struct {
	sshClient *remote.SSHClient
}

// NewSSHNodeProvider creates a new NodeProvider backed by SSH.
func NewSSHNodeProvider(client *remote.SSHClient) node.NodeProvider {
	return &SSHNodeProvider{sshClient: client}
}

// Ping checks if the remote node is reachable via SSH.
func (n *SSHNodeProvider) Ping(ctx context.Context) error {
	log.G(ctx).Debug("Node ping")
	_, err := n.sshClient.Exec(ctx, "echo ok")
	if err != nil {
		log.G(ctx).WithError(err).Warn("Node ping failed")
		return err
	}
	log.G(ctx).Debug("Node ping succeeded")
	return nil
}

// NotifyNodeStatus is called to register a callback for node status changes.
// The default node status is static; override this to dynamically monitor the remote node.
func (n *SSHNodeProvider) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	log.G(ctx).Info("NotifyNodeStatus registered (static)")
}
