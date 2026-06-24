package config

// Config holds configuration for connecting to a remote node via SSH.
type Config struct {
	// Host is the remote node's IP or hostname.
	Host string
	// Port is the SSH port (default 22).
	Port int
	// User is the SSH login user.
	User string
	// SSHKeyPath is the path to the SSH private key file.
	SSHKeyPath string
	// WorkDir is the root directory on the remote node for storing pod data.
	// Each pod gets a subdirectory: <WorkDir>/<namespace>/<podName>/
	WorkDir string
}

// DefaultPort returns 22 if port is 0.
func (c *Config) DefaultPort() int {
	if c.Port == 0 {
		return 22
	}
	return c.Port
}
