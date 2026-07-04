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
		WaitingFor: wait.ForListeningPort("22/tcp"),
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
