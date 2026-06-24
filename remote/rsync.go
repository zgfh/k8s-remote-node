package remote

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"golang.org/x/crypto/ssh"
)

// SyncClient uploads files to a remote node via SFTP.
type SyncClient struct {
	host    string
	port    int
	user    string
	keyPath string
}

// NewSyncClient creates a new file sync client.
func NewSyncClient(host string, port int, user, keyPath string) *SyncClient {
	return &SyncClient{
		host:    host,
		port:    port,
		user:    user,
		keyPath: keyPath,
	}
}

// ExtraFile represents an additional file to upload alongside docker-compose.yml.
type ExtraFile struct {
	Path    string // relative path within the pod directory
	Content []byte
}

// Upload creates the pod directory on the remote node and uploads all files.
// composeContent is the docker-compose.yml content.
// extraFiles are additional files (configmaps, secrets) to upload.
func (c *SyncClient) Upload(ctx context.Context, targetDir string, composeContent []byte, extraFiles []ExtraFile) error {
	log.G(ctx).WithFields(log.Fields{
		"target_dir":     targetDir,
		"compose_bytes":  len(composeContent),
		"extra_files":    len(extraFiles),
	}).Info("SFTP upload starting")

	client, err := c.sftpClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create directory tree
	if err := client.MkdirAll(targetDir); err != nil {
		log.G(ctx).WithError(err).WithField("dir", targetDir).Error("SFTP mkdir failed")
		return fmt.Errorf("mkdir %s: %w", targetDir, err)
	}

	// Upload docker-compose.yml
	composePath := filepath.Join(targetDir, "docker-compose.yml")
	if err := c.writeFile(ctx, client, composePath, composeContent); err != nil {
		return fmt.Errorf("upload docker-compose.yml: %w", err)
	}

	// Upload extra files
	for _, f := range extraFiles {
		fullPath := filepath.Join(targetDir, f.Path)
		dir := filepath.Dir(fullPath)
		if err := client.MkdirAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("dir", dir).Error("SFTP mkdir failed")
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		if err := c.writeFile(ctx, client, fullPath, f.Content); err != nil {
			return fmt.Errorf("upload %s: %w", f.Path, err)
		}
	}

	log.G(ctx).WithField("target_dir", targetDir).Info("SFTP upload complete")
	return nil
}

func (c *SyncClient) sftpClient(ctx context.Context) (*sftp.Client, error) {
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	log.G(ctx).WithField("addr", addr).Debug("SFTP connecting")

	key, err := os.ReadFile(c.keyPath)
	if err != nil {
		log.G(ctx).WithError(err).Error("SFTP failed to read key file")
		return nil, fmt.Errorf("read ssh key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.G(ctx).WithError(err).Error("SFTP failed to parse private key")
		return nil, fmt.Errorf("parse ssh key: %w", err)
	}

	config := &ssh.ClientConfig{
		User:            c.user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		log.G(ctx).WithError(err).WithField("addr", addr).Error("SFTP dial failed")
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		log.G(ctx).WithError(err).Error("SFTP create client failed")
		return nil, fmt.Errorf("sftp client: %w", err)
	}
	log.G(ctx).WithField("addr", addr).Debug("SFTP connected")
	return client, nil
}

func (c *SyncClient) writeFile(ctx context.Context, client *sftp.Client, path string, content []byte) error {
	log.G(ctx).WithFields(log.Fields{
		"path": path,
		"size": len(content),
	}).Debug("SFTP writing file")

	f, err := client.Create(path)
	if err != nil {
		log.G(ctx).WithError(err).WithField("path", path).Error("SFTP create file failed")
		return err
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}
