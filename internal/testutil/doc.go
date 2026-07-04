// Package testutil provides reusable testcontainers-go helpers
// (StartPostgres, StartRedis) so every integration test across the
// project shares one container-lifecycle implementation instead of each
// hand-rolling its own. See plans/docs/16-development-workflow.md §16.4
// and plans/task/core/17.
package testutil
