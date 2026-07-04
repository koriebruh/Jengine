package sftp

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// EnvSecretResolver resolves a "vault path ref" by mapping it to an
// environment variable name - a stand-in until plans/task/core/23 builds
// a real Vault client. A vaultPathRef of "secret/data/sftp/tenant-x/password"
// maps to env var SECRET_DATA_SFTP_TENANT_X_PASSWORD (non-alphanumeric
// runs collapsed to a single underscore, uppercased). Suitable for local
// dev/testing only - never point this at a production deployment.
type EnvSecretResolver struct{}

func (EnvSecretResolver) Resolve(ctx context.Context, vaultPathRef string) (string, error) {
	envName := toEnvName(vaultPathRef)
	v, ok := os.LookupEnv(envName)
	if !ok {
		return "", fmt.Errorf("sftp: EnvSecretResolver: no env var %s set for vault path ref %q", envName, vaultPathRef)
	}
	return v, nil
}

func toEnvName(vaultPathRef string) string {
	var b strings.Builder
	prevUnderscore := false
	for _, r := range strings.ToUpper(vaultPathRef) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
