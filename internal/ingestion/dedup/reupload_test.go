package dedup_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
)

type fakeChecksumChecker struct {
	exists bool
	err    error
}

func (f *fakeChecksumChecker) ExistsByChecksum(ctx context.Context, tenantID, accountID uuid.UUID, checksum string) (bool, error) {
	return f.exists, f.err
}

type fakePolicyLookup struct {
	policy dedup.ReuploadPolicy
	err    error
}

func (f *fakePolicyLookup) GetReuploadPolicy(ctx context.Context, tenantID uuid.UUID) (dedup.ReuploadPolicy, error) {
	return f.policy, f.err
}

func TestCheckFileReupload_NotAReuploadWhenChecksumUnseen(t *testing.T) {
	statements := &fakeChecksumChecker{exists: false}
	settings := &fakePolicyLookup{policy: dedup.ReuploadPolicyReject}

	policy, isReupload, err := dedup.CheckFileReupload(context.Background(), statements, settings, uuid.New(), uuid.New(), "checksum-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isReupload {
		t.Fatal("expected isReupload=false for an unseen checksum")
	}
	if policy != "" {
		t.Errorf("expected no policy decision for a non-reupload, got %q", policy)
	}
}

func TestCheckFileReupload_RejectPolicy(t *testing.T) {
	statements := &fakeChecksumChecker{exists: true}
	settings := &fakePolicyLookup{policy: dedup.ReuploadPolicyReject}

	policy, isReupload, err := dedup.CheckFileReupload(context.Background(), statements, settings, uuid.New(), uuid.New(), "checksum-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReupload {
		t.Fatal("expected isReupload=true for a seen checksum")
	}
	if policy != dedup.ReuploadPolicyReject {
		t.Errorf("expected reject policy, got %q", policy)
	}
}

func TestCheckFileReupload_CorrectionPolicy(t *testing.T) {
	statements := &fakeChecksumChecker{exists: true}
	settings := &fakePolicyLookup{policy: dedup.ReuploadPolicyTreatAsCorrection}

	policy, isReupload, err := dedup.CheckFileReupload(context.Background(), statements, settings, uuid.New(), uuid.New(), "checksum-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReupload {
		t.Fatal("expected isReupload=true for a seen checksum")
	}
	if policy != dedup.ReuploadPolicyTreatAsCorrection {
		t.Errorf("expected correction policy, got %q", policy)
	}
}

func TestCheckFileReupload_UnsetPolicyDefaultsToReject(t *testing.T) {
	statements := &fakeChecksumChecker{exists: true}
	settings := &fakePolicyLookup{policy: ""}

	policy, isReupload, err := dedup.CheckFileReupload(context.Background(), statements, settings, uuid.New(), uuid.New(), "checksum-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isReupload {
		t.Fatal("expected isReupload=true")
	}
	if policy != dedup.ReuploadPolicyReject {
		t.Errorf("expected the safe default (reject) when unset, got %q", policy)
	}
}
