package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// OPAClient evaluates an authorization decision. The only production
// implementation talks to a real OPA sidecar (`opa run --server`,
// policy bundle from deploy/opa/policies/) over its REST API - this
// interface exists so tests/mocks don't need a running OPA process.
type OPAClient interface {
	Evaluate(ctx context.Context, input OPAInput) (Decision, error)
}

// HTTPOPAClient calls a real OPA server's Data API
// (POST {baseURL}/v1/data/jengine/authz/allow).
type HTTPOPAClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewHTTPOPAClient(baseURL string) *HTTPOPAClient {
	return &HTTPOPAClient{BaseURL: baseURL, HTTPClient: http.DefaultClient}
}

type opaDataRequest struct {
	Input OPAInput `json:"input"`
}

type opaDataResponse struct {
	Result *bool `json:"result"`
}

func (c *HTTPOPAClient) Evaluate(ctx context.Context, input OPAInput) (Decision, error) {
	body, err := json.Marshal(opaDataRequest{Input: input})
	if err != nil {
		return Decision{}, fmt.Errorf("authz: marshal OPA input: %w", err)
	}

	url := c.BaseURL + "/v1/data/jengine/authz/allow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Decision{}, fmt.Errorf("authz: build OPA request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return Decision{}, fmt.Errorf("authz: call OPA: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return Decision{}, fmt.Errorf("authz: OPA returned status %d", resp.StatusCode)
	}

	var out opaDataResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Decision{}, fmt.Errorf("authz: decode OPA response: %w", err)
	}

	// OPA's Data API returns `"result": null` (no `allow` value derived
	// at all, e.g. undefined rule) rather than `false` when nothing
	// matches under some evaluation paths - treat absence the same as
	// an explicit deny, never as an implicit allow.
	if out.Result == nil || !*out.Result {
		return Decision{Allow: false, Reason: fmt.Sprintf("denied by policy for action %q", input.Action)}, nil
	}
	return Decision{Allow: true}, nil
}

var _ OPAClient = (*HTTPOPAClient)(nil)
