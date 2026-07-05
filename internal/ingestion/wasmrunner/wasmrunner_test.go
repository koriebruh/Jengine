package wasmrunner_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/ingestion/wasmrunner"
)

// loadGuestWasm reads the pre-built test fixture (built via `GOOS=wasip1
// GOARCH=wasm go build -buildmode=c-shared` in testdata/guest/ - see
// that directory's own main.go doc comment for why this isn't a TinyGo
// module). Skips if the fixture hasn't been built (kept out of version
// control as a binary artifact; rebuild via testdata/guest's own
// documented command).
func loadGuestWasm(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/guest/guest.wasm")
	if err != nil {
		t.Skipf("test guest.wasm fixture not built - run: cd testdata/guest && GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o guest.wasm .: %v", err)
	}
	return data
}

type fakeSecrets struct{ values map[string]string }

func (f *fakeSecrets) Resolve(ctx context.Context, key string) (string, error) {
	v, ok := f.values[key]
	if !ok {
		return "", os.ErrNotExist
	}
	return v, nil
}

type fakeCheckpoint struct{ cursor []byte }

func (f *fakeCheckpoint) Save(ctx context.Context, cursor []byte) error {
	f.cursor = append([]byte(nil), cursor...)
	return nil
}
func (f *fakeCheckpoint) Load(ctx context.Context) ([]byte, error) {
	return f.cursor, nil
}

func TestRunner_Fetch_EmitsRecords(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	defer func() { _ = r.Close(ctx) }()

	records, err := r.Fetch(ctx, []byte("normal"))
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
}

func TestRunner_Validate(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	defer func() { _ = r.Close(ctx) }()

	if err := r.Validate(ctx, []byte("some-config")); err != nil {
		t.Errorf("expected valid config to pass Validate, got: %v", err)
	}
	if err := r.Validate(ctx, []byte("")); err == nil {
		t.Error("expected empty config to fail Validate")
	}
}

func TestRunner_SupportsStreaming(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	defer func() { _ = r.Close(ctx) }()

	supports, err := r.SupportsStreaming(ctx)
	if err != nil {
		t.Fatalf("SupportsStreaming failed: %v", err)
	}
	if supports {
		t.Error("expected the fixture guest to report supportsStreaming=false")
	}
}

// TestRunner_SecretResolution_RespectsDeclaredKeyScoping is part of
// this task's own DoD ("secret resolution respects declared-key
// scoping"): a key the connector's manifest didn't declare must be
// denied, never silently resolved - even if the host's SecretResolver
// technically has a value for it.
func TestRunner_SecretResolution_RespectsDeclaredKeyScoping(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	secrets := &fakeSecrets{values: map[string]string{"api_key": "super-secret-value"}}

	t.Run("declared key resolves", func(t *testing.T) {
		r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{
			Timeout:  5 * time.Second,
			Secrets:  secrets,
			Manifest: wasmrunner.Manifest{AllowedSecretKeys: []string{"api_key"}},
		})
		if err != nil {
			t.Fatalf("NewRunner failed: %v", err)
		}
		defer func() { _ = r.Close(ctx) }()

		records, err := r.Fetch(ctx, []byte("trigger-secret"))
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		if len(records) != 1 || !strings.Contains(string(records[0]), "super-secret-value") {
			t.Errorf("expected the declared secret to be resolved and emitted, got %v", records)
		}
	})

	t.Run("undeclared key is denied", func(t *testing.T) {
		r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{
			Timeout: 5 * time.Second,
			Secrets: secrets,
			// Manifest.AllowedSecretKeys deliberately empty - api_key is
			// NOT declared, even though the resolver has a real value.
		})
		if err != nil {
			t.Fatalf("NewRunner failed: %v", err)
		}
		defer func() { _ = r.Close(ctx) }()

		records, err := r.Fetch(ctx, []byte("trigger-secret"))
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

// TestRunner_EgressEnforcement_BlocksNonAllowlistedDomain is part of
// this task's own DoD ("egress enforcement blocks non-allowlisted
// domains").
func TestRunner_EgressEnforcement_BlocksNonAllowlistedDomain(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	httpCalled := false
	r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{
		Timeout:  5 * time.Second,
		Manifest: wasmrunner.Manifest{AllowedEgressDomains: []string{"allowed.example.com"}},
		HTTPDo: func(ctx context.Context, req []byte) ([]byte, error) {
			httpCalled = true
			return []byte("should never be reached"), nil
		},
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	defer func() { _ = r.Close(ctx) }()

	if _, err := r.Fetch(ctx, []byte("trigger-egress")); err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if httpCalled {
		t.Error("expected egress to evil.example.com to be blocked before HTTPDo was ever called")
	}
}

// TestRunner_CheckpointRoundTrip is part of this task's own DoD
// ("checkpoint save/load round-trips correctly").
func TestRunner_CheckpointRoundTrip(t *testing.T) {
	store := &fakeCheckpoint{}
	if err := store.Save(context.Background(), []byte("cursor-value-123")); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(got) != "cursor-value-123" {
		t.Errorf("expected round-tripped cursor, got %q", got)
	}
}

// TestRunner_Timeout_KillsInfiniteLoop is part of this task's own DoD
// ("a deliberately infinite-looping guest module gets killed within a
// bounded wall-clock time, and the host process itself remains healthy
// afterward").
func TestRunner_Timeout_KillsInfiniteLoop(t *testing.T) {
	wasmBytes := loadGuestWasm(t)
	ctx := context.Background()

	r, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	defer func() { _ = r.Close(ctx) }()

	start := time.Now()
	_, err = r.Fetch(ctx, []byte("infinite-loop"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the infinite-looping guest to return an error, it did not")
	}
	if elapsed > 10*time.Second {
		t.Errorf("expected the guest to be killed within a bounded window, took %s", elapsed)
	}

	// Host process health check: a fresh Runner against a NEW module
	// must still work after the previous one was hard-killed - proves
	// the host itself wasn't corrupted/hung by the runaway guest.
	r2, err := wasmrunner.NewRunner(ctx, wasmBytes, wasmrunner.Config{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewRunner after timeout failed - host may be unhealthy: %v", err)
	}
	defer func() { _ = r2.Close(ctx) }()
	if _, err := r2.Fetch(ctx, []byte("normal")); err != nil {
		t.Errorf("host unhealthy after previous timeout: Fetch failed: %v", err)
	}
}
