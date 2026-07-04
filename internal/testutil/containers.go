package testutil

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDB wraps a running Postgres testcontainer and a ready-to-use pool.
type TestDB struct {
	DSN  string
	Pool *pgxpool.Pool
}

// StartPostgres starts a Postgres testcontainer, applies any migrations
// found under migrations/*.sql (plans/task/core/03 populates these - if
// none exist yet, the container just starts with no schema, which is
// fine), and returns a ready-to-use connection pool. Registers
// t.Cleanup to tear the container and pool down.
//
// Applying the real migrations/*.sql files (not a hand-maintained
// test-only schema copy) is the point of testcontainers-go per
// plans/docs/16-development-workflow.md §16.4 - a schema fork here would
// defeat "testing against real infra behavior."
func StartPostgres(t *testing.T) *TestDB {
	t.Helper()
	ctx := context.Background()

	opts := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("jengine_test"),
		postgres.WithUsername("jengine"),
		postgres.WithPassword("jengine_test"),
		// The official postgres image restarts once after initdb, logging
		// "database system is ready to accept connections" twice. Without
		// this explicit wait strategy the module can report ready after
		// the FIRST occurrence (mid-restart), causing a real but flaky
		// "connection reset by peer" on the first query - reproduced
		// during plans/task/core/17 verification. See the testcontainers-go
		// postgres module docs' own wait-strategy example.
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	}

	if migrationFiles := findMigrationFiles(t); len(migrationFiles) > 0 {
		opts = append(opts, postgres.WithInitScripts(migrationFiles...))
	}

	pgContainer, err := postgres.Run(ctx, "postgres:16", opts...)
	testcontainers.CleanupContainer(t, pgContainer)
	if err != nil {
		t.Fatalf("testutil: failed to start postgres container: %v", err)
	}

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("testutil: failed to get postgres connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("testutil: failed to create pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return &TestDB{DSN: dsn, Pool: pool}
}

// TestRedis wraps a running Redis testcontainer and a ready-to-use client.
type TestRedis struct {
	Addr   string
	Client *redis.Client
}

// StartRedis starts a Redis testcontainer and returns a ready-to-use
// client. Registers t.Cleanup to tear down.
func StartRedis(t *testing.T) *TestRedis {
	t.Helper()
	ctx := context.Background()

	redisContainer, err := tcredis.Run(ctx, "redis:7")
	testcontainers.CleanupContainer(t, redisContainer)
	if err != nil {
		t.Fatalf("testutil: failed to start redis container: %v", err)
	}

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("testutil: failed to get redis connection string: %v", err)
	}

	opt, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("testutil: failed to parse redis connection string %q: %v", connStr, err)
	}
	client := redis.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })

	return &TestRedis{Addr: opt.Addr, Client: client}
}

// findMigrationFiles locates <repo-root>/migrations/*.up.sql regardless of
// which package's test calls StartPostgres, by walking up from the
// current working directory to the directory containing go.mod. Only
// *.up.sql is matched - *.down.sql must never be applied at container
// init (it would immediately reverse the schema the up migrations just
// created).
func findMigrationFiles(t *testing.T) []string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		return nil
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			matches, err := filepath.Glob(filepath.Join(dir, "migrations", "*.up.sql"))
			if err != nil {
				return nil
			}
			sort.Strings(matches)
			return matches
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil // reached filesystem root without finding go.mod
		}
		dir = parent
	}
}
