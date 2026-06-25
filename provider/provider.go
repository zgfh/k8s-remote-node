package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	dto "github.com/prometheus/client_model/go"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	"github.com/zgfh/k8s-remote-node/config"
	"github.com/zgfh/k8s-remote-node/remote"
)

// ComposeProvider implements nodeutil.Provider and node.PodNotifier.
//
// It translates Kubernetes Pod specs into docker-compose.yml files,
// uploads them via SFTP to a remote node, and manages containers
// via docker compose commands over SSH.
type ComposeProvider struct {
	cfg             config.Config
	configMapLister corev1listers.ConfigMapLister
	secretLister    corev1listers.SecretLister
	sshClient       *remote.SSHClient
	syncClient      *remote.SyncClient
	notifyFunc      func(*corev1.Pod)
	pods            sync.Map // namespace/name → *corev1.Pod
}

// Ensure interface compliance at compile time.
var _ nodeutil.Provider = (*ComposeProvider)(nil)

// NewComposeProvider creates a new provider for managing pods on a remote node.
// SSH connection is established lazily on first use.
func NewComposeProvider(cfg config.Config, pc nodeutil.ProviderConfig) (*ComposeProvider, error) {
	return &ComposeProvider{
		cfg:             cfg,
		configMapLister: pc.ConfigMaps,
		secretLister:    pc.Secrets,
		sshClient:       remote.NewSSHClient(cfg.Host, cfg.DefaultPort(), cfg.User, cfg.SSHKeyPath),
		syncClient:      remote.NewSyncClient(cfg.Host, cfg.DefaultPort(), cfg.User, cfg.SSHKeyPath),
	}, nil
}

// SSHClient returns the underlying SSH client, used to create the NodeProvider.
func (p *ComposeProvider) SSHClient() *remote.SSHClient {
	return p.sshClient
}

// podDir returns the working directory for a pod on the remote node.
func (p *ComposeProvider) podDir(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s/%s", p.cfg.WorkDir, pod.Namespace, pod.Name)
}

func podKey(ns, name string) string {
	return ns + "/" + name
}

// ─── PodLifecycleHandler ────────────────────────────────────────────────────

// CreatePod deploys a Pod on the remote node via docker compose.
func (p *ComposeProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	logger := log.G(ctx).WithFields(log.Fields{
		"pod":        pod.Namespace + "/" + pod.Name,
		"uid":        string(pod.UID),
		"containers": len(pod.Spec.Containers),
		"host":       p.cfg.Host,
	})
	logger.Info("CreatePod started")

	dir := p.podDir(pod)
	logger.WithField("dir", dir).Debug("Target directory")

	// 1. Convert pod to docker-compose.yml
	composeBytes, err := ConvertToCompose(ctx, pod)
	if err != nil {
		logger.WithError(err).Error("Failed to convert pod to compose")
		return errdefs.InvalidInputf("convert pod to compose: %v", err)
	}
	logger.WithField("compose_yaml", string(composeBytes)).Debug("Generated compose")

	// 2. Resolve volumes (configmaps, secrets) into extra files
	extraFiles, err := p.resolveVolumes(ctx, pod)
	if err != nil {
		logger.WithError(err).Error("Failed to resolve volumes")
		return errdefs.InvalidInputf("resolve volumes: %v", err)
	}

	// 3. Upload all files to remote node via SFTP
	if err := p.syncClient.Upload(ctx, dir, composeBytes, extraFiles); err != nil {
		logger.WithError(err).WithField("dir", dir).Error("SFTP upload failed")
		return fmt.Errorf("upload files: %w", err)
	}
	logger.Info("Files uploaded")

	// 4. Start containers via docker compose
	cmd := fmt.Sprintf("cd %s && docker compose up -d 2>&1", dir)
	logger.WithField("cmd", cmd).Info("Running docker compose up")
	out, err := p.sshClient.Exec(ctx, cmd)
	if err != nil {
		logger.WithError(err).WithField("output", string(out)).Error("docker compose up failed")
		return fmt.Errorf("docker compose up: %s: %w", string(out), err)
	}
	logger.WithField("output", string(out)).Info("docker compose up succeeded")

	// 5. Cache pod as Running
	cached := pod.DeepCopy()
	cached.Status = corev1.PodStatus{
		Phase:     corev1.PodRunning,
		StartTime: &metav1.Time{Time: time.Now()},
	}
	for _, c := range pod.Spec.Containers {
		cached.Status.ContainerStatuses = append(cached.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			Ready:   true,
			Started: boolPtr(true),
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()},
			},
		})
	}
	p.pods.Store(podKey(pod.Namespace, pod.Name), cached)

	// 6. Notify framework to update K8s pod status
	if p.notifyFunc != nil {
		p.notifyFunc(cached)
	}
	logger.Info("CreatePod completed")
	return nil
}

// UpdatePod re-deploys a pod on the remote node.
func (p *ComposeProvider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	logger := log.G(ctx).WithField("pod", pod.Namespace+"/"+pod.Name)
	logger.Info("UpdatePod started (delete + recreate)")
	if err := p.DeletePod(ctx, pod); err != nil {
		logger.WithError(err).Error("UpdatePod: delete failed")
		return err
	}
	if err := p.CreatePod(ctx, pod); err != nil {
		logger.WithError(err).Error("UpdatePod: create failed")
		return err
	}
	logger.Info("UpdatePod completed")
	return nil
}

// DeletePod removes a pod from the remote node.
func (p *ComposeProvider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	logger := log.G(ctx).WithFields(log.Fields{
		"pod":  pod.Namespace + "/" + pod.Name,
		"uid":  string(pod.UID),
		"host": p.cfg.Host,
	})
	logger.Info("DeletePod started")

	dir := p.podDir(pod)
	cmd := fmt.Sprintf("cd %s && docker compose down -v 2>&1; rm -rf %s", dir, dir)
	out, err := p.sshClient.Exec(ctx, cmd)
	if err != nil {
		logger.WithError(err).WithField("output", string(out)).Error("docker compose down failed")
		return fmt.Errorf("docker compose down: %s: %w", string(out), err)
	}
	logger.WithField("output", string(out)).Info("docker compose down succeeded")

	p.pods.Delete(podKey(pod.Namespace, pod.Name))
	logger.Info("DeletePod completed")
	return nil
}

// GetPod retrieves a cached pod by namespace and name.
func (p *ComposeProvider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	key := podKey(namespace, name)
	v, ok := p.pods.Load(key)
	if !ok {
		log.G(ctx).WithField("pod", key).Debug("GetPod: not found")
		return nil, errdefs.NotFoundf("pod %s not found", key)
	}
	log.G(ctx).WithField("pod", key).Debug("GetPod: found")
	return v.(*corev1.Pod).DeepCopy(), nil
}

// GetPodStatus queries the remote docker compose status and returns the pod status.
func (p *ComposeProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	logger := log.G(ctx).WithField("pod", namespace+"/"+name)
	dir := fmt.Sprintf("%s/%s/%s", p.cfg.WorkDir, namespace, name)
	out, err := p.sshClient.Exec(ctx, fmt.Sprintf("cd %s && docker compose ps --format json 2>&1", dir))
	if err != nil {
		logger.WithError(err).Debug("GetPodStatus: docker compose ps failed, returning Failed")
		return &corev1.PodStatus{
			Phase: corev1.PodFailed,
		}, nil
	}
	status := parseComposeStatus(ctx, out)
	logger.WithFields(log.Fields{
		"phase":         status.Phase,
		"container_cnt": len(status.ContainerStatuses),
	}).Debug("GetPodStatus")
	return status, nil
}

// GetPods returns all cached pods.
func (p *ComposeProvider) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	var result []*corev1.Pod
	count := 0
	p.pods.Range(func(_, v interface{}) bool {
		result = append(result, v.(*corev1.Pod).DeepCopy())
		count++
		return true
	})
	log.G(ctx).WithField("count", count).Debug("GetPods")
	return result, nil
}

// ─── PodNotifier ────────────────────────────────────────────────────────────

// NotifyPods registers a callback and starts background status synchronization.
func (p *ComposeProvider) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	log.G(ctx).Info("NotifyPods registered, starting status sync loop")
	p.notifyFunc = cb
	go p.statusSyncLoop(ctx, cb)
}

func (p *ComposeProvider) statusSyncLoop(ctx context.Context, cb func(*corev1.Pod)) {
	log.G(ctx).Info("Status sync loop started (interval=30s)")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.G(ctx).Info("Status sync loop stopped")
			return
		case <-ticker.C:
			p.pods.Range(func(_, v interface{}) bool {
				pod := v.(*corev1.Pod)
				podLogger := log.G(ctx).WithField("pod", pod.Namespace+"/"+pod.Name)
				status, err := p.GetPodStatus(ctx, pod.Namespace, pod.Name)
				if err != nil {
					podLogger.WithError(err).Warn("Status sync: failed to get status")
					return true
				}
				updated := pod.DeepCopy()
				updated.Status = *status
				p.pods.Store(podKey(pod.Namespace, pod.Name), updated)
				cb(updated)
				podLogger.WithField("phase", status.Phase).Debug("Status synced")
				return true
			})
		}
	}
}

// ─── Container Logs / Exec / Attach ────────────────────────────────────────

// GetContainerLogs retrieves logs from the remote container via docker compose logs.
func (p *ComposeProvider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	logger := log.G(ctx).WithFields(log.Fields{
		"pod":         namespace + "/" + podName,
		"container":   containerName,
		"tail":        opts.Tail,
		"timestamps":  opts.Timestamps,
		"since_secs":  opts.SinceSeconds,
	})
	logger.Info("GetContainerLogs")

	dir := fmt.Sprintf("%s/%s/%s", p.cfg.WorkDir, namespace, podName)
	tail := ""
	if opts.Tail > 0 {
		tail = fmt.Sprintf(" --tail %d", opts.Tail)
	}
	cmd := fmt.Sprintf("cd %s && docker compose logs%s %s 2>&1", dir, tail, containerName)
	out, err := p.sshClient.Exec(ctx, cmd)
	if err != nil {
		logger.WithError(err).Error("GetContainerLogs failed")
		return nil, errdefs.NotFoundf("logs for %s/%s: %v", podName, containerName, err)
	}
	logger.WithField("bytes", len(out)).Debug("GetContainerLogs succeeded")
	return io.NopCloser(bytes.NewReader(out)), nil
}

// RunInContainer executes a command in a remote container via docker compose exec.
func (p *ComposeProvider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	logger := log.G(ctx).WithFields(log.Fields{
		"pod":       namespace + "/" + podName,
		"container": containerName,
		"cmd":       cmd,
	})
	logger.Info("RunInContainer")

	dir := fmt.Sprintf("%s/%s/%s", p.cfg.WorkDir, namespace, podName)
	execCmd := fmt.Sprintf("cd %s && docker compose exec -T %s %s 2>&1", dir, containerName, joinCmd(cmd))
	logger.WithField("exec_cmd", execCmd).Debug("Running remote exec")

	out, err := p.sshClient.Exec(ctx, execCmd)
	if err != nil {
		logger.WithError(err).WithField("output", string(out)).Error("RunInContainer failed")
		return fmt.Errorf("exec in container: %w", err)
	}
	attach.Stdout().Write(out)
	logger.WithField("output_len", len(out)).Debug("RunInContainer succeeded")
	return nil
}

// AttachToContainer is not directly supported over SSH; returns an error.
func (p *ComposeProvider) AttachToContainer(ctx context.Context, namespace, podName, containerName string, attach api.AttachIO) error {
	log.G(ctx).WithField("pod", namespace+"/"+podName).Warn("AttachToContainer not supported over SSH")
	return fmt.Errorf("attach not supported over SSH")
}

// GetStatsSummary returns empty stats (remote node stats not collected).
func (p *ComposeProvider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error) {
	return &statsv1alpha1.Summary{}, nil
}

// GetMetricsResource returns empty metrics.
func (p *ComposeProvider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error) {
	return nil, nil
}

// PortForward forwards a port to the remote container via SSH port forwarding.
func (p *ComposeProvider) PortForward(ctx context.Context, namespace, pod string, port int32, stream io.ReadWriteCloser) error {
	log.G(ctx).WithFields(log.Fields{
		"pod":  namespace + "/" + pod,
		"port": port,
	}).Warn("PortForward not implemented")
	return fmt.Errorf("port-forward not implemented: use SSH directly to the remote node")
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// resolveVolumes extracts configmap and secret data from a pod spec
// and returns them as ExtraFiles for uploading to the remote node.
func (p *ComposeProvider) resolveVolumes(ctx context.Context, pod *corev1.Pod) ([]remote.ExtraFile, error) {
	logger := log.G(ctx).WithField("pod", pod.Namespace+"/"+pod.Name)
	var files []remote.ExtraFile

	for _, vol := range pod.Spec.Volumes {
		switch {
		case vol.ConfigMap != nil:
			cm, err := p.configMapLister.ConfigMaps(pod.Namespace).Get(vol.ConfigMap.Name)
			if err != nil {
				logger.WithError(err).WithField("configmap", vol.ConfigMap.Name).Error("Failed to fetch ConfigMap")
				return nil, fmt.Errorf("fetch configmap %s/%s: %w", pod.Namespace, vol.ConfigMap.Name, err)
			}
			for key, data := range cm.Data {
				if !includeVolumeKey(vol.ConfigMap.Items, key) {
					continue
				}
				relPath := filepath.Join("volumes", vol.Name, key)
				files = append(files, remote.ExtraFile{
					Path:    relPath,
					Content: []byte(data),
				})
				logger.WithFields(log.Fields{
					"configmap": vol.ConfigMap.Name,
					"key":       key,
					"path":      relPath,
				}).Debug("Resolved ConfigMap key")
			}
			for key, data := range cm.BinaryData {
				if !includeVolumeKey(vol.ConfigMap.Items, key) {
					continue
				}
				relPath := filepath.Join("volumes", vol.Name, key)
				files = append(files, remote.ExtraFile{
					Path:    relPath,
					Content: data,
				})
				logger.WithFields(log.Fields{
					"configmap": vol.ConfigMap.Name,
					"key":       key,
					"path":      relPath,
				}).Debug("Resolved ConfigMap binary key")
			}
		case vol.Secret != nil:
			secret, err := p.secretLister.Secrets(pod.Namespace).Get(vol.Secret.SecretName)
			if err != nil {
				logger.WithError(err).WithField("secret", vol.Secret.SecretName).Error("Failed to fetch Secret")
				return nil, fmt.Errorf("fetch secret %s/%s: %w", pod.Namespace, vol.Secret.SecretName, err)
			}
			for key, data := range secret.Data {
				if !includeVolumeKey(vol.Secret.Items, key) {
					continue
				}
				relPath := filepath.Join("volumes", vol.Name, key)
				files = append(files, remote.ExtraFile{
					Path:    relPath,
					Content: data,
				})
				logger.WithFields(log.Fields{
					"secret": vol.Secret.SecretName,
					"key":    key,
					"path":   relPath,
				}).Debug("Resolved Secret key")
			}
		case vol.EmptyDir != nil:
			dirName := filepath.Join("volumes", vol.Name)
			files = append(files, remote.ExtraFile{
				Path:    filepath.Join(dirName, ".gitkeep"),
				Content: []byte{},
			})
			logger.WithField("emptydir", vol.Name).Debug("Created EmptyDir placeholder")
		default:
			logger.WithField("volume", vol.Name).Debug("Unknown volume type")
		}
	}
	logger.WithField("file_count", len(files)).Info("Volumes resolved")
	return files, nil
}

// includeVolumeKey checks whether a key should be included based on the optional
// items list in the volume source. If items is empty or nil, all keys are included.
func includeVolumeKey(items []corev1.KeyToPath, key string) bool {
	if len(items) == 0 {
		return true
	}
	for _, item := range items {
		if item.Key == key {
			return true
		}
	}
	return false
}

// composeContainer represents a single container entry from "docker compose ps --format json".
type composeContainer struct {
	Name       string `json:"Name"`
	Command    string `json:"Command"`
	Project    string `json:"Project"`
	Service    string `json:"Service"`
	State      string `json:"State"`
	ExitCode   int    `json:"ExitCode"`
	Publishers []struct {
		URL           string `json:"URL"`
		TargetPort    int    `json:"TargetPort"`
		PublishedPort int    `json:"PublishedPort"`
		Protocol      string `json:"Protocol"`
	} `json:"Publishers"`
}

// parseComposeStatus parses the JSON output of "docker compose ps --format json"
// and converts it to a Kubernetes PodStatus.
func parseComposeStatus(ctx context.Context, out []byte) *corev1.PodStatus {
	status := &corev1.PodStatus{
		Phase: corev1.PodRunning,
	}

	var containers []composeContainer

	// docker compose ps --format json outputs a JSON array: [{...}, {...}]
	if err := json.Unmarshal(out, &containers); err != nil {
		// Fallback: try line-by-line (jsonlines format)
		lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			var c composeContainer
			if err := json.Unmarshal(line, &c); err != nil {
				log.G(ctx).WithError(err).WithField("line", string(line)).Warn("Failed to parse compose ps output")
				continue
			}
			containers = append(containers, c)
		}
	}

	if len(containers) == 0 {
		status.Phase = corev1.PodPending
		log.G(ctx).Debug("parseComposeStatus: no containers found, phase=Pending")
		return status
	}

	hasRunning := false
	for _, c := range containers {
		cs := corev1.ContainerStatus{
			Name:  c.Service,
			Ready: c.State == "running",
		}
		switch c.State {
		case "running":
			hasRunning = true
			cs.State = corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{},
			}
			cs.Ready = true
		case "exited":
			if c.ExitCode == 0 {
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: int32(c.ExitCode),
						Reason:   "Completed",
					},
				}
			} else {
				cs.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: int32(c.ExitCode),
						Reason:   "Error",
					},
				}
			}
		default:
			cs.State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason: c.State,
				},
			}
		}
		status.ContainerStatuses = append(status.ContainerStatuses, cs)
	}

	if !hasRunning {
		status.Phase = corev1.PodFailed
	}

	return status
}

func boolPtr(b bool) *bool {
	return &b
}

// joinCmd joins a command slice into a shell-safe string.
func joinCmd(cmd []string) string {
	if len(cmd) == 0 {
		return ""
	}
	result := ""
	for _, arg := range cmd {
		result += fmt.Sprintf("%q ", arg)
	}
	return result[:len(result)-1]
}
