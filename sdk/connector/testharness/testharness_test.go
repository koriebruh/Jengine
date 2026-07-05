package testharness_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/koriebruh/jengine-connector-sdk/testharness"
)

// loadGuestWasm reads the pre-built test fixture (built via `GOOS=wasip1
// GOARCH=wasm go build -buildmode=c-shared` in testdata/guest/ - see
// that directory's own main.go doc comment for why this isn't a TinyGo
// module). Skips if the fixture hasn't been built (kept out of version
// control as a binary artifact).
func loadGuestWasm(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/guest/guest.wasm")
	if err != nil {
		t.Skipf("test guest.wasm fixture not built - run: cd testdata/guest && GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o guest.wasm .: %v", err)
	}
	return data
}

func TestHarness_Fetch_EmitsRecords(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	records, err := h.Fetch(ctx, []byte("normal"))
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 emitted records, got %d: %v", len(records), records)
	}
	if string(records[0]) != `{"id":"rec-1","amount":"100.00"}` {
		t.Errorf("unexpected record[0]: %s", records[0])
	}
	if string(records[1]) != `{"id":"rec-2","amount":"200.00"}` {
		t.Errorf("unexpected record[1]: %s", records[1])
	}

	logs := h.Logs()
	found := false
	for _, l := range logs {
		if strings.Contains(l, "fetch called with config: normal") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a log line recording the fetch call, got %v", logs)
	}
}

func TestHarness_Validate(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	if err := h.Validate(ctx, []byte("some-config")); err != nil {
		t.Errorf("expected valid config to pass Validate, got: %v", err)
	}
	if err := h.Validate(ctx, []byte("")); err == nil {
		t.Error("expected empty config to fail Validate")
	}
}

func TestHarness_SupportsStreaming(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	supports, err := h.SupportsStreaming(ctx)
	if err != nil {
		t.Fatalf("SupportsStreaming failed: %v", err)
	}
	if supports {
		t.Error("expected the fixture guest to report supportsStreaming=false")
	}
}

// TestHarness_SecretResolution_RespectsDeclaredKeyScoping mirrors
// wasmrunner's own DoD test: a key the connector's manifest didn't
// declare (i.e. never passed into MockSecrets/allowed here) must never
// resolve, even though this fixture always asks for "api_key".
func TestHarness_SecretResolution_RespectsDeclaredKeyScoping(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	t.Run("declared key resolves", func(t *testing.T) {
		h, err := testharness.New(ctx, wasmBytes, testharness.Options{
			Timeout: 5 * time.Second,
			Secrets: testharness.MockSecrets{"api_key": "super-secret-value"},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = h.Close(ctx) }()

		records, err := h.Fetch(ctx, []byte("trigger-secret"))
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		if len(records) != 1 || !strings.Contains(string(records[0]), "super-secret-value") {
			t.Errorf("expected the secret to be resolved and emitted, got %v", records)
		}
	})

	t.Run("undeclared key is denied", func(t *testing.T) {
		h, err := testharness.New(ctx, wasmBytes, testharness.Options{
			Timeout: 5 * time.Second,
			// Secrets deliberately empty/nil - api_key has no value here.
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer func() { _ = h.Close(ctx) }()

		records, err := h.Fetch(ctx, []byte("trigger-secret"))
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		for _, rec := range records {
			if strings.Contains(string(rec), "super-secret-value") {
				t.Fatalf("undeclared secret key was resolved and leaked: %s", rec)
			}
		}
	})
}

// TestHarness_EgressEnforcement_BlocksNonAllowlistedDomain: the mock
// hostHTTPFetch never dispatches a real request even for an allowed
// domain (see host.go's own doc comment), so this only asserts the
// non-allowlisted case never crashes the guest and never emits
// anything resembling a real response.
func TestHarness_EgressEnforcement_BlocksNonAllowlistedDomain(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{
		Timeout:              5 * time.Second,
		AllowedEgressDomains: []string{"allowed.example.com"},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	if _, err := h.Fetch(ctx, []byte("trigger-egress")); err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	for _, rec := range h.EmittedRecords() {
		if strings.Contains(string(rec), "should never be reached") {
			t.Fatalf("egress to a non-allowlisted domain was not blocked: %s", rec)
		}
	}
}

// TestHarness_Timeout_KillsInfiniteLoop mirrors wasmrunner's own DoD
// test: a deliberately infinite-looping guest gets killed within a
// bounded wall-clock time, and the host stays healthy afterward.
func TestHarness_Timeout_KillsInfiniteLoop(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	start := time.Now()
	_, err = h.Fetch(ctx, []byte("infinite-loop"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the infinite-looping guest to return an error, it did not")
	}
	if elapsed > 10*time.Second {
		t.Errorf("expected the guest to be killed within a bounded window, took %s", elapsed)
	}

	h2, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New after timeout failed - host may be unhealthy: %v", err)
	}
	defer func() { _ = h2.Close(ctx) }()
	if _, err := h2.Fetch(ctx, []byte("normal")); err != nil {
		t.Errorf("host unhealthy after previous timeout: Fetch failed: %v", err)
	}
}

func TestGolden_CompareAndWrite(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = h.Close(ctx) }()

	records, err := h.Fetch(ctx, []byte("normal"))
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	goldenPath := t.TempDir() + "/expected.json"
	if err := testharness.WriteGolden(records, goldenPath); err != nil {
		t.Fatalf("WriteGolden failed: %v", err)
	}
	if err := testharness.CompareGolden(records, goldenPath); err != nil {
		t.Errorf("CompareGolden against its own freshly-written fixture failed: %v", err)
	}

	if err := testharness.CompareGolden(records[:1], goldenPath); err == nil {
		t.Error("expected a record-count mismatch to fail CompareGolden")
	}
}
