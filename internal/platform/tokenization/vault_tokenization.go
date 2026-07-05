package tokenization

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// VaultTokenizationService implements TokenizationService against a
// real Vault KV v2 secrets engine (local dev: docker-compose's `vault`
// service, dev mode). Vault is a genuinely separate store from
// Postgres (this task's own explicit requirement), reachable only via
// its own API/token - not a table this codebase's own Postgres
// connection pool could accidentally join against the tokenized data.
type VaultTokenizationService struct {
	BaseURL    string // e.g. http://localhost:8200
	Token      string
	HTTPClient *http.Client
}

func NewVaultTokenizationService(baseURL, token string) *VaultTokenizationService {
	return &VaultTokenizationService{BaseURL: baseURL, Token: token, HTTPClient: http.DefaultClient}
}

// tokenPrefix makes every token structurally distinct from a valid
// PAN-shaped string (13-19 digits) - this task's own DoD requires this
// explicitly, not just "looks different in practice."
const tokenPrefix = "tok_"

func (s *VaultTokenizationService) Tokenize(ctx context.Context, tenantID, field, value string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("tokenization: generate token: %w", err)
	}
	token := tokenPrefix + hex.EncodeToString(raw)

	path := s.vaultPath(tenantID, token)
	body, err := json.Marshal(map[string]any{"data": map[string]string{"value": value, "field": field}})
	if err != nil {
		return "", fmt.Errorf("tokenization: marshal vault payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("tokenization: build vault request: %w", err)
	}
	req.Header.Set("X-Vault-Token", s.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("tokenization: write to vault: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tokenization: vault write returned status %d", resp.StatusCode)
	}

	return token, nil
}

func (s *VaultTokenizationService) Detokenize(ctx context.Context, tenantID, token string) (string, error) {
	path := s.vaultPath(tenantID, token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.BaseURL+path, nil)
	if err != nil {
		return "", fmt.Errorf("tokenization: build vault request: %w", err)
	}
	req.Header.Set("X-Vault-Token", s.Token)

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("tokenization: read from vault: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("tokenization: token not found")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tokenization: vault read returned status %d", resp.StatusCode)
	}

	var out struct {
		Data struct {
			Data struct {
				Value string `json:"value"`
			} `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("tokenization: decode vault response: %w", err)
	}
	return out.Data.Data.Value, nil
}

// vaultPath namespaces every token under the tenant's own KV path -
// secret/data/tokens/<tenant>/<token> for the KV v2 data endpoint.
func (s *VaultTokenizationService) vaultPath(tenantID, token string) string {
	return "/v1/secret/data/tokens/" + tenantID + "/" + token
}

var _ TokenizationService = (*VaultTokenizationService)(nil)
