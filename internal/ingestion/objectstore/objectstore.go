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
)

// ObjectStore is the minimal read/write surface connectors need.
type ObjectStore interface {
	Get(ctx context.Context, bucket, key string) ([]byte, error)
	Put(ctx context.Context, bucket, key string, data []byte) error
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

var _ ObjectStore = (*MinIOStore)(nil)
