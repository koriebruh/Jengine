package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// nullableTime returns nil for a zero time.Time so callers can rely on a
// SQL-side COALESCE(..., now()) or COALESCE(..., NULL) default instead of
// sending an invalid zero-value timestamp.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// jsonOrEmptyObject returns b, or a literal "{}" if b is empty. Used when
// building rows for bulkInsertChunked: unlike the single-row INSERT
// statements elsewhere in this package, bulkInsertChunked's generated SQL
// has no per-column COALESCE (it doesn't know which columns are
// NOT NULL jsonb with a default) - so a NOT NULL jsonb column's default
// must already be resolved in Go before the row reaches it, or Postgres
// rejects a literal NULL.
func jsonOrEmptyObject(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}

// bulkInsertChunked inserts rows into table using multi-row
// INSERT ... VALUES (...), (...), ..., chunked to stay under Postgres'
// 65535-parameter-per-statement limit.
//
// This is NOT pgx.CopyFrom: Postgres does not support COPY FROM on
// tables with row-level security enabled ("ERROR: COPY FROM not
// supported with row-level security") - discovered empirically while
// verifying plans/task/core/05 against the real RLS-enabled schema from
// task 03, not assumed. plans/docs/04-matching-engine.md §5.5 itself
// names multi-row INSERT as an accepted alternative to COPY for the
// batch-upsert performance requirement, so this is the one used
// everywhere a bulk write touches a tenant-scoped (RLS'd) table.
func bulkInsertChunked(ctx context.Context, tx pgx.Tx, table string, columns []string, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	const maxParams = 60000 // stay safely under Postgres' 65535-per-statement limit
	chunkSize := maxParams / len(columns)
	if chunkSize < 1 {
		chunkSize = 1
	}

	total := 0
	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		chunk := rows[start:end]

		var sb strings.Builder
		sb.WriteString("INSERT INTO ")
		sb.WriteString(table)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(columns, ", "))
		sb.WriteString(") VALUES ")

		args := make([]any, 0, len(chunk)*len(columns))
		paramIdx := 1
		for i, row := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for j := range row {
				if j > 0 {
					sb.WriteString(", ")
				}
				fmt.Fprintf(&sb, "$%d", paramIdx)
				paramIdx++
			}
			sb.WriteString(")")
			args = append(args, row...)
		}

		tag, err := tx.Exec(ctx, sb.String(), args...)
		if err != nil {
			return total, fmt.Errorf("bulk insert into %s: %w", table, err)
		}
		total += int(tag.RowsAffected())
	}
	return total, nil
}
