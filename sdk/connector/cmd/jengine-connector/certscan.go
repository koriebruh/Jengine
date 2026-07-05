package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/koriebruh/jengine-connector-sdk/testharness"
)

type connectorManifest struct {
	Name                 string   `json:"name"`
	Version              string   `json:"version"`
	AllowedSecretKeys    []string `json:"allowed_secret_keys"`
	AllowedEgressDomains []string `json:"allowed_egress_domains"`
}

// runCertScan is the code-facing slice of "certification" (plans/task/core/25):
// an automated pre-submission checklist. The human marketplace review
// and listing process itself is explicitly NOT part of this task.
func runCertScan(args []string) error {
	manifestBytes, err := os.ReadFile("manifest.json")
	if err != nil {
		return fmt.Errorf("read manifest.json: %w", err)
	}
	var manifest connectorManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("parse manifest.json: %w", err)
	}

	wasmBytes, err := os.ReadFile("connector.wasm")
	if err != nil {
		return fmt.Errorf("read connector.wasm (did you run `jengine-connector build`?): %w", err)
	}

	var failures []string

	// --- Static check: no known-dangerous import module names ---
	// A byte-level substring scan, NOT a full wasm binary parser - this
	// can only catch a KNOWN dangerous pattern appearing literally in
	// the binary (e.g. an older non-sandboxed WASI preview, or a raw
	// libc/syscall-shaped import module name), it cannot enumerate
	// every import the module actually declares. Treat this as a
	// coarse tripwire, not a substitute for wasmrunner's own runtime
	// enforcement (which only ever wires up jengine_host + the WASI
	// shim regardless of what a guest tries to import - an
	// unrecognized import simply fails to link at load time there).
	for _, dangerous := range []string{"wasi_unstable", "GOT.mem", "GOT.func"} {
		if bytes.Contains(wasmBytes, []byte(dangerous)) {
			failures = append(failures, fmt.Sprintf("binary references %q - only jengine_host and wasi_snapshot_preview1 imports are permitted at runtime", dangerous))
		}
	}

	// --- Static check: manifest declares at least a plausible shape ---
	if manifest.Name == "" {
		failures = append(failures, "manifest.json: name is empty")
	}

	// --- Dynamic check: run the harness, verify egress + secret
	// scoping, and that no secret-shaped string reached log() ---
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testSecrets := testharness.MockSecrets{}
	for _, key := range manifest.AllowedSecretKeys {
		testSecrets[key] = "CERTSCAN-CANARY-" + key
	}

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{
		Secrets:              testSecrets,
		AllowedEgressDomains: manifest.AllowedEgressDomains,
		Timeout:              10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("load connector for dynamic scan: %w", err)
	}
	defer func() { _ = h.Close(ctx) }()

	input, err := os.ReadFile("testdata/sample_input.json")
	if err != nil {
		return fmt.Errorf("read testdata/sample_input.json: %w", err)
	}
	if _, err := h.Fetch(ctx, input); err != nil {
		// A Fetch error during cert-scan isn't itself a security
		// failure - `jengine-connector test` is what asserts
		// correctness. Only the log-content checks below matter here.
		fmt.Fprintf(os.Stderr, "note: Fetch returned an error during cert-scan (not a scan failure by itself): %v\n", err)
	}

	for _, key := range manifest.AllowedSecretKeys {
		canary := "CERTSCAN-CANARY-" + key
		for _, line := range h.Logs() {
			if strings.Contains(line, canary) {
				failures = append(failures, fmt.Sprintf("secret-shaped value for declared key %q was passed to log() - never log resolved secret values", key))
			}
		}
	}

	if len(failures) > 0 {
		fmt.Println("cert-scan FAILED:")
		for _, f := range failures {
			fmt.Println("  -", f)
		}
		return fmt.Errorf("%d cert-scan check(s) failed", len(failures))
	}
	fmt.Println("cert-scan PASSED (code-facing checks only - human marketplace review still applies before listing)")
	return nil
}
