package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestSFTP wraps a running SFTP testcontainer (atmoz/sftp, the same
// image used for the local dev stack's sftp service - plans/task/core/07)
// and the host directory bind-mounted as its upload directory, so tests
// can add/modify files there between poll cycles and have the container
// see the change immediately (a real bind mount, not a one-time file
// copy - required for the "second poll picks up a newly added file"
// scenario).
type TestSFTP struct {
	Host     string // "host:port"
	Username string
	Password string
	HostDir  string // host-side path bind-mounted into the container's upload dir
}

// StartSFTP starts an isolated SFTP testcontainer (not the shared local
// dev-stack one) bind-mounting hostDir as the upload directory. Registers
// t.Cleanup to tear down.
func StartSFTP(t *testing.T, hostDir string) *TestSFTP {
	t.Helper()
	ctx := context.Background()

	const user = "testuser"
	const password = "testpass"

	req := testcontainers.ContainerRequest{
		Image:        "atmoz/sftp:latest",
		ExposedPorts: []string{"22/tcp"},
		Cmd:          []string{fmt.Sprintf("%s:%s:1001:1001:upload", user, password)},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = []string{fmt.Sprintf("%s:/home/%s/upload", hostDir, user)}
		},
		// wait.ForListeningPort alone is racy for this image: the port
		// accepts TCP connections briefly before sshd has finished
		// generating host keys/chrooting the upload user, so an SSH
		// handshake attempted right after "port open" can get a real
		// "connection reset by peer" - reproduced on a CI runner (GitHub
		// Actions), not just a theoretical race. atmoz/sftp's OpenSSH
		// logs this exact line once it's actually ready to negotiate.
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("22/tcp"),
			wait.ForLog("Server listening on"),
		),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start sftp container: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get sftp container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "22/tcp")
	if err != nil {
		t.Fatalf("failed to get sftp container port: %v", err)
	}

	return &TestSFTP{
		Host:     fmt.Sprintf("%s:%s", host, port.Port()),
		Username: user,
		Password: password,
		HostDir:  hostDir,
	}
}
