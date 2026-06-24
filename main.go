package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logrusadapter "github.com/virtual-kubelet/virtual-kubelet/log/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	"github.com/zgfh/k8s-remote-node/config"
	"github.com/zgfh/k8s-remote-node/provider"
)

func init() {
	// Configure logrus as the backend for virtual-kubelet logging
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	})
	level, err := logrus.ParseLevel(getEnv("LOG_LEVEL", "info"))
	if err != nil {
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
	log.L = logrusadapter.FromLogrus(logrus.NewEntry(logrus.StandardLogger()))
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.Config{
		Host:       getEnv("SSH_HOST", ""),
		Port:       getEnvInt("SSH_PORT", 22),
		User:       getEnv("SSH_USER", "root"),
		SSHKeyPath: getEnv("SSH_KEY_PATH", "/etc/ssh-key/id_rsa"),
		WorkDir:    getEnv("WORK_DIR", "/opt/vk-pods"),
	}

	if cfg.Host == "" {
		fmt.Fprintf(os.Stderr, "FATAL: SSH_HOST environment variable is required\n")
		os.Exit(1)
	}

	nodeName := getEnv("VK_NODE_NAME", "ssh-node-"+cfg.Host)

	tlsCertFile := getEnv("TLS_CERT_FILE", "/etc/vk-tls/tls.crt")
	tlsKeyFile := getEnv("TLS_KEY_FILE", "/etc/vk-tls/tls.key")

	nodeSpec := buildNodeSpec(nodeName)
	nodeSpec.Status.Capacity = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(getEnv("NODE_CAPACITY_CPU", "8")),
		corev1.ResourceMemory: resource.MustParse(getEnv("NODE_CAPACITY_MEMORY", "16Gi")),
		corev1.ResourcePods:   resource.MustParse(getEnv("NODE_CAPACITY_PODS", "100")),
	}

	log.G(ctx).WithFields(log.Fields{
		"node_name":   nodeName,
		"ssh_host":    cfg.Host,
		"ssh_port":    cfg.DefaultPort(),
		"ssh_user":    cfg.User,
		"ssh_key":     cfg.SSHKeyPath,
		"work_dir":    cfg.WorkDir,
		"listen_addr": getEnv("LISTEN_ADDR", ":10250"),
		"tls_enabled": tlsFilesExist(tlsCertFile, tlsKeyFile),
		"cpu_cap":     nodeSpec.Status.Capacity.Cpu().String(),
		"mem_cap":     nodeSpec.Status.Capacity.Memory().String(),
		"pods_cap":    nodeSpec.Status.Capacity.Pods().String(),
	}).Info("Virtual-kubelet SSH provider starting")

	kubeClient, err := nodeutil.ClientsetFromEnv(getEnv("KUBECONFIG", ""))
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: failed to create kubernetes client: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	n, err := nodeutil.NewNode(
		nodeName,
		func(pc nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			p, err := provider.NewComposeProvider(cfg)
			if err != nil {
				log.G(ctx).WithError(err).Error("Failed to create ComposeProvider")
				return nil, nil, fmt.Errorf("create provider: %w", err)
			}
			np := provider.NewSSHNodeProvider(p.SSHClient())
			if pc.Node != nil {
				pc.Node.Status.Capacity = nodeSpec.Status.Capacity
				pc.Node.Status.NodeInfo = nodeSpec.Status.NodeInfo
			}
			log.G(ctx).Info("Provider initialized")
			return p, np, nil
		},
		nodeutil.WithNodeConfig(nodeutil.NodeConfig{
			NodeSpec:              nodeSpec,
			HTTPListenAddr:        getEnv("LISTEN_ADDR", ":10250"),
			TLSConfig:             loadTLSConfig(tlsCertFile, tlsKeyFile),
			Handler:               mux,
			NumWorkers:            1,
			InformerResyncPeriod:  0,
			StreamIdleTimeout:     0,
			StreamCreationTimeout: 0,
		}),
		nodeutil.WithClient(kubeClient),
		nodeutil.AttachProviderRoutes(mux),
	)
	if err != nil {
		log.G(ctx).WithError(err).Error("Failed to create node")
		fmt.Fprintf(os.Stderr, "FATAL: failed to create node: %v\n", err)
		os.Exit(1)
	}

	log.G(ctx).Info("Node created, starting controllers")

	if err := n.Run(ctx); err != nil {
		log.G(ctx).WithError(err).Error("Node exited with error")
		fmt.Fprintf(os.Stderr, "FATAL: node exited with error: %v\n", err)
		os.Exit(1)
	}

	log.G(ctx).Info("Node exited normally")
}

func tlsFilesExist(cert, key string) bool {
	_, e1 := os.Stat(cert)
	_, e2 := os.Stat(key)
	return e1 == nil && e2 == nil
}

func buildNodeSpec(name string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"type":                   "ssh-compose",
				"beta.kubernetes.io/os":  "linux",
				"kubernetes.io/role":     "agent",
				"kubernetes.io/hostname": name,
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "virtual-kubelet.io/provider",
					Value:  "ssh-compose",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				OperatingSystem: "linux",
				Architecture:    "amd64",
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

func loadTLSConfig(certFile, keyFile string) *tls.Config {
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		return nil
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: failed to load TLS cert: %v (TLS disabled)\n", err)
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
