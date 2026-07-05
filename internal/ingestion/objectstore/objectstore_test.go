package objectstore_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/objectstore"
)

const localMinIOAddr = "localhost:9000"

func requireLocalMinIO(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localMinIOAddr, 2*time.Second)
	if err != nil {
		t.Skipf("local MinIO not reachable at %s (run `make dev-up`): %v", localMinIOAddr, err)
	}
	_ = conn.Close()
}

func TestMinIOStore_Put_RoundTrip(t *testing.T) {
	requireLocalMinIO(t)

	store, err := objectstore.NewMinIOStore(localMinIOAddr, "jengine", "jengine_dev_secret", false)
	if err != nil {
		t.Fatalf("NewMinIOStore failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "test/" + uuid.New().String() + ".txt"
	want := []byte("plain object body")

	if err := store.Put(ctx, "jengine-statements", key, want); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	got, err := store.Get(ctx, "jengine-statements", key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestMinIOStore_PutEncrypted_RequiresKMSBackend documents the real,
// observed behavior against this local dev stack's plain MinIO (no
// KES/KMS backend configured - plans/task/core/23's own PutEncrypted
// doc comment: standing one up is infra-team work, out of this task's
// code scope, same category as the service-mesh Non-Goal). SSE-KMS
// against a MinIO deployment with no KMS backend fails server-side -
// this test pins down that it fails LOUDLY (a clear error), not
// silently falling back to an unencrypted write.
func TestMinIOStore_PutEncrypted_RequiresKMSBackend(t *testing.T) {
	requireLocalMinIO(t)

	store, err := objectstore.NewMinIOStore(localMinIOAddr, "jengine", "jengine_dev_secret", false)
	if err != nil {
		t.Fatalf("NewMinIOStore failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "test/" + uuid.New().String() + ".txt"
	err = store.PutEncrypted(ctx, "jengine-statements", key, []byte("sensitive body"), "test-kek-reference")
	if err == nil {
		t.Fatal("expected PutEncrypted to fail against a MinIO deployment with no KMS backend configured, it did not")
	}
	t.Logf("PutEncrypted against a KMS-less MinIO failed as expected: %v", err)
}
