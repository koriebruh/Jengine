// Package clickhouse is the analytics query layer (plans/task/core/22)
// reading the three materialized views deploy/clickhouse/ddl.sql
// defines: mv_match_rate_by_rule, mv_sla_compliance,
// mv_breaks_daily_aging. No MVP-era Postgres-aggregate reporting
// endpoint exists in this codebase to repoint (plans/docs/11-scalability-roadmap.md
// §12.2 Phase 0's "Postgres materialized views acceptable at MVP scale"
// was never actually built) - this package is the query layer for
// whatever dashboard/reporting consumer (frontend Overview Dashboard,
// future GraphQL gateway) is built against it, per this task's own
// scope: "only the query layer/API it consumes," not the dashboard
// itself.
package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// NewClient connects to ClickHouse's native protocol port (9000 in
// production; the local dev docker-compose service maps it to 9004 to
// avoid a MinIO port collision - see docker-compose.yaml's own comment).
func NewClient(ctx context.Context, addr, database, username, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return conn, nil
}
