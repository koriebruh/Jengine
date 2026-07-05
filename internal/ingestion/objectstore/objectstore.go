// Package objectstore is a thin MinIO/S3 adapter for reading already-
// uploaded source files - plans/task/core/07's csvupload/sftp connectors
// consume file bytes through the ObjectStore interface here rather than
// depending on minio-go directly, consistent with this codebase's
// pattern of keeping third-party client libraries behind a small local
// interface (see internal/ingestion/kafka's franz-go adapter).
package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

// ObjectStore is the minimal read/write surface connectors need.
//
// PutEncrypted (plans/task/core/23 §10.1) is how statement-file/audit-
// archive writes use SSE-KMS with the tenant's own KEK - kmsKeyID is
// the tenant's kek_reference (migrations/0014), envelope-encrypting
// the object server-side under that key rather than the bucket's
// default key. Plain Put remains for callers with no per-tenant key to
// scope to (e.g. this package's own demo/test fixtures).
type ObjectStore interface {
	Get(ctx context.Context, bucket, key string) ([]byte, error)
	Put(ctx context.Context, bucket, key string, data []byte) error
	PutEncrypted(ctx context.Context, bucket, key string, data []byte, kmsKeyID string) error
}

// MinIOStore implements ObjectStore against a MinIO/S3-compatible
// endpoint (local dev stack: plans/task/core/02's docker-compose MinIO
// service).
type MinIOStore struct {
	client *minio.Client
}

func NewMinIOStore(endpoint, accessKey, secretKey string, useSSL bool) (*MinIOStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("objectstore: new minio client: %w", err)
	}
	return &MinIOStore{client: client}, nil
}

func (s *MinIOStore) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("objectstore: get %s/%s: %w", bucket, key, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("objectstore: read %s/%s: %w", bucket, key, err)
	}
	return data, nil
}

func (s *MinIOStore) Put(ctx context.Context, bucket, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("objectstore: put %s/%s: %w", bucket, key, err)
	}
	return nil
}

// PutEncrypted writes with SSE-KMS, envelope-encrypting under kmsKeyID
// (the tenant's KEK reference) rather than any bucket-default key -
// plans/task/core/23 §10.1, plans/docs/01-multi-tenancy.md §2.3's
// per-tenant KEK actually getting used at write time. Requires the
// target MinIO/S3 deployment to have a KMS backend configured (e.g.
// MinIO KES, or AWS S3's native SSE-KMS) - the local dev docker-compose
// MinIO does NOT have one wired up (a KES + Vault/KMS backend stand-up
// is infra-team work, the same category this task's own Non-Goals
// already carves out for service-mesh installation), so PutEncrypted
// against local dev MinIO will surface a real server-side error rather
// than silently falling back to unencrypted - see this task's
// integration test and QA_REPORT.md for what that error looks like in
// practice and what finishing this wiring in a real deployment needs.
func (s *MinIOStore) PutEncrypted(ctx context.Context, bucket, key string, data []byte, kmsKeyID string) error {
	sse, err := encrypt.NewSSEKMS(kmsKeyID, nil)
	if err != nil {
		return fmt.Errorf("objectstore: build SSE-KMS config for key %q: %w", kmsKeyID, err)
	}
	_, err = s.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ServerSideEncryption: sse,
	})
	if err != nil {
		return fmt.Errorf("objectstore: put %s/%s with SSE-KMS key %q: %w", bucket, key, kmsKeyID, err)
	}
	return nil
}

var _ ObjectStore = (*MinIOStore)(nil)
