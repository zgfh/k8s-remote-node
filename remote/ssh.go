package remote

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"golang.org/x/crypto/ssh"
)

// SSHClient manages an SSH connection to a remote node.
// The connection is established lazily on first use and automatically reconnects if dropped.
type SSHClient struct {
	host    string
	port    int
	user    string
	keyPath string

	mu     sync.Mutex
	client *ssh.Client
}

// NewSSHClient creates a new SSH client. The actual connection is deferred
// until the first Exec call.
func NewSSHClient(host string, port int, user, keyPath string) *SSHClient {
	return &SSHClient{
		host:    host,
		port:    port,
		user:    user,
		keyPath: keyPath,
	}
}

// Addr returns the remote address string.
func (c *SSHClient) Addr() string {
	return fmt.Sprintf("%s@%s:%d", c.user, c.host, c.port)
}

func (c *SSHClient) connect(ctx context.Context) error {
	log.G(ctx).WithFields(log.Fields{
		"addr":     c.Addr(),
		"key_path": c.keyPath,
	}).Info("SSH connecting")

	key, err := os.ReadFile(c.keyPath)
	if err != nil {
		log.G(ctx).WithError(err).Error("SSH failed to read key file")
		return fmt.Errorf("read ssh key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.G(ctx).WithError(err).Error("SSH failed to parse private key")
		return fmt.Errorf("parse ssh key: %w", err)
	}

	config := &ssh.ClientConfig{
		User:            c.user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		log.G(ctx).WithError(err).WithFields(log.Fields{
			"addr": addr,
		}).Error("SSH dial failed")
		return fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	c.client = client
	log.G(ctx).WithFields(log.Fields{"addr": addr}).Info("SSH connected")
	return nil
}

// Exec runs a command on the remote node and returns its combined output.
// It auto-connects if not already connected.
func (c *SSHClient) Exec(ctx context.Context, cmd string) ([]byte, error) {
	logger := log.G(ctx).WithField("cmd", truncate(cmd, 200))
	logger.Debug("SSH exec")

	c.mu.Lock()
	if c.client == nil {
		if err := c.connect(ctx); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	client := c.client
	c.mu.Unlock()

	session, err := client.NewSession()
	if err != nil {
		logger.WithError(err).Warn("SSH session failed, reconnecting")
		// Connection might be stale, try reconnect once
		c.mu.Lock()
		c.client = nil
		if err2 := c.connect(ctx); err2 != nil {
			c.mu.Unlock()
			logger.WithError(err2).Error("SSH reconnect failed")
			return nil, fmt.Errorf("create session: %w (reconnect: %v)", err, err2)
		}
		client = c.client
		c.mu.Unlock()

		session, err = client.NewSession()
		if err != nil {
			logger.WithError(err).Error("SSH create session failed after reconnect")
			return nil, fmt.Errorf("create session after reconnect: %w", err)
		}
		logger.Info("SSH reconnected successfully")
	}
	defer session.Close()

	// Use separate buffers because the SSH package copies stdout and stderr
	// from concurrent goroutines. bytes.Buffer is not safe for concurrent writes.
	var stdoutBuf, stderrBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stderrBuf

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	var execErr error
	select {
	case <-ctx.Done():
		session.Signal(ssh.SIGKILL)
		logger.Warn("SSH exec cancelled by context")
		// Merge partial output for the cancelled case too
		var buf bytes.Buffer
		buf.Write(stdoutBuf.Bytes())
		buf.Write(stderrBuf.Bytes())
		return buf.Bytes(), ctx.Err()
	case execErr = <-done:
	}

	// Merge stdout and stderr after both goroutines have finished
	var buf bytes.Buffer
	buf.Write(stdoutBuf.Bytes())
	buf.Write(stderrBuf.Bytes())

	if execErr != nil {
		logger.WithFields(log.Fields{
			"output": truncate(buf.String(), 500),
		}).WithError(execErr).Warn("SSH exec returned error")
	} else {
		logger.WithField("output_len", buf.Len()).Debug("SSH exec succeeded")
	}

	return buf.Bytes(), execErr
}

// Close closes the SSH connection.
func (c *SSHClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		return err
	}
	return nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
