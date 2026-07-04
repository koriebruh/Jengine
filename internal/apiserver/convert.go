package apiserver

import (
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// jsonMarshal wraps json.Marshal with context in the error message -
// used for building AuditEvent.AfterState payloads from plain maps.
func jsonMarshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("apiserver: marshal json: %w", err)
	}
	return b, nil
}

// parseUUID wraps uuid.Parse with a connect.CodeInvalidArgument error -
// every handler taking an ID string from a request needs this.
func parseUUID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: invalid %s %q: %w", field, s, err))
	}
	return id, nil
}

func toTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func fromTimestamp(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
