package provider

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"gopkg.in/yaml.v3"
)

// ComposeFile represents a docker-compose.yml structure.
type ComposeFile struct {
	Services map[string]ComposeService `yaml:"services"`
}

// ComposeService represents a single service in docker-compose.
type ComposeService struct {
	Image       string         `yaml:"image"`
	Environment []string       `yaml:"environment,omitempty"`
	Ports       []string       `yaml:"ports,omitempty"`
	Volumes     []string       `yaml:"volumes,omitempty"`
	Restart     string         `yaml:"restart"`
	Deploy      *ComposeDeploy `yaml:"deploy,omitempty"`
	DependsOn   []string       `yaml:"depends_on,omitempty"`
	Command     []string       `yaml:"command,omitempty"`
	Entrypoint  []string       `yaml:"entrypoint,omitempty"`
	NetworkMode string         `yaml:"network_mode,omitempty"`
}

// ComposeDeploy specifies deployment resource constraints.
type ComposeDeploy struct {
	Resources ComposeResources `yaml:"resources"`
}

// ComposeResources holds resource limits.
type ComposeResources struct {
	Limits ComposeResourceSpec `yaml:"limits"`
}

// ComposeResourceSpec defines CPU and memory limits.
type ComposeResourceSpec struct {
	CPUs   string `yaml:"cpus,omitempty"`
	Memory string `yaml:"memory,omitempty"`
}

// ConvertToCompose converts a Kubernetes Pod spec into a docker-compose.yml.
func ConvertToCompose(ctx context.Context, pod *corev1.Pod) ([]byte, error) {
	logger := log.G(ctx).WithFields(log.Fields{
		"pod":       pod.Namespace + "/" + pod.Name,
		"containers": len(pod.Spec.Containers),
		"init_ctrs": len(pod.Spec.InitContainers),
	})
	logger.Info("Converting pod to docker-compose")

	compose := ComposeFile{
		Services: make(map[string]ComposeService),
	}

	// Convert init containers first (they become one-shot services that main services depend on)
	var initNames []string
	for _, c := range pod.Spec.InitContainers {
		svc := convertContainer(c, pod, true)
		name := "init-" + c.Name
		compose.Services[name] = svc
		initNames = append(initNames, name)
		logger.WithField("init_container", c.Name).Debug("Converted init container")
	}

	// Convert main containers
	for _, c := range pod.Spec.Containers {
		svc := convertContainer(c, pod, false)
		if len(initNames) > 0 {
			svc.DependsOn = initNames
		}
		compose.Services[c.Name] = svc
		logger.WithField("container", c.Name).Debug("Converted container")
	}

	result, err := yaml.Marshal(compose)
	if err != nil {
		logger.WithError(err).Error("Failed to marshal docker-compose YAML")
		return nil, err
	}
	logger.WithField("yaml_bytes", len(result)).Info("Pod converted to docker-compose")
	return result, nil
}

func convertContainer(c corev1.Container, pod *corev1.Pod, isInit bool) ComposeService {
	svc := ComposeService{
		Image:   c.Image,
		Restart: restartPolicy(pod.Spec.RestartPolicy, isInit),
	}

	// hostNetwork maps to docker network_mode: host
	if pod.Spec.HostNetwork {
		svc.NetworkMode = "host"
	}

	// Environment variables
	for _, env := range c.Env {
		svc.Environment = append(svc.Environment, fmt.Sprintf("%s=%s", env.Name, env.Value))
	}

	// Ports (skip when hostNetwork=true; network_mode: host doesn't support port mapping)
	if !pod.Spec.HostNetwork {
		for _, p := range c.Ports {
			if p.HostPort > 0 {
				svc.Ports = append(svc.Ports, fmt.Sprintf("%d:%d", p.HostPort, p.ContainerPort))
			} else {
				svc.Ports = append(svc.Ports, fmt.Sprintf("%d", p.ContainerPort))
			}
		}
	}

	// Volume mounts
	for _, vm := range c.VolumeMounts {
		srcPath := volumeSourcePath(vm.Name, pod)
		// When subPath is set, mount the specific file/subdirectory within the volume
		if vm.SubPath != "" {
			srcPath = srcPath + "/" + vm.SubPath
		}
		ro := ""
		if vm.ReadOnly {
			ro = ":ro"
		}
		svc.Volumes = append(svc.Volumes,
			fmt.Sprintf("%s:%s%s", srcPath, vm.MountPath, ro))
	}

	// Command and args — escape $ to $$ so docker-compose doesn't
	// interpret shell variables (e.g. ${VERSION}) as host env vars.
	if len(c.Command) > 0 {
		svc.Entrypoint = make([]string, len(c.Command))
		for i, s := range c.Command {
			svc.Entrypoint[i] = escapeDollar(s)
		}
	}
	if len(c.Args) > 0 {
		svc.Command = make([]string, len(c.Args))
		for i, s := range c.Args {
			svc.Command[i] = escapeDollar(s)
		}
	}

	// Resource limits
	limits := c.Resources.Limits
	if limits != nil {
		cpu := limits.Cpu()
		mem := limits.Memory()
		if (cpu != nil && !cpu.IsZero()) || (mem != nil && !mem.IsZero()) {
			svc.Deploy = &ComposeDeploy{
				Resources: ComposeResources{
					Limits: ComposeResourceSpec{},
				},
			}
			if cpu != nil && !cpu.IsZero() {
				svc.Deploy.Resources.Limits.CPUs = fmt.Sprintf("%.2f", float64(cpu.MilliValue())/1000)
			}
			if mem != nil && !mem.IsZero() {
				svc.Deploy.Resources.Limits.Memory = fmt.Sprintf("%dM", mem.Value()/1024/1024)
			}
		}
	}

	return svc
}

func restartPolicy(p corev1.RestartPolicy, isInit bool) string {
	if isInit {
		return "on-failure"
	}
	switch p {
	case corev1.RestartPolicyAlways:
		return "unless-stopped"
	case corev1.RestartPolicyOnFailure:
		return "on-failure"
	default:
		return "no"
	}
}

// volumeSourcePath returns the source path for a volume on the remote host.
// For HostPath volumes, it returns the host path directly.
// For ConfigMap/Secret/EmptyDir volumes, it returns the path under ./volumes/
// where the provider places uploaded files.
func volumeSourcePath(volumeName string, pod *corev1.Pod) string {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == volumeName {
			if vol.HostPath != nil {
				return vol.HostPath.Path
			}
			break
		}
	}
	return "./volumes/" + volumeName
}

// escapeDollar escapes $ as $$ so docker-compose variable interpolation
// passes a literal $ through to the container shell.
func escapeDollar(s string) string {
	return strings.ReplaceAll(s, "$", "$$")
}
