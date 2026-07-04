package pipeline_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/testconnector"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// fakeStage is a trivial named pass-through, standing in for the real
// stages tasks 07-09 implement (Format Parse, Field Mapping,
// Normalization, Validation, Dedup, Canonicalization) - this task's own
// tests only need to prove the orchestrator calls stages in order and
// handles Quarantine/Drop correctly, not exercise real per-format logic.
type fakeStage struct {
	name   string
	result pipeline.StageResult
	err    error
}

func (s *fakeStage) Name() string { return s.name }
func (s *fakeStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	return s.result, s.err
}

func passthroughStages(names ...string) []pipeline.Stage {
	stages := make([]pipeline.Stage, len(names))
	for i, n := range names {
		stages[i] = &fakeStage{name: n, result: pipeline.StageContinue}
	}
	return stages
}

// The 8 named stages per plans/docs/02-data-ingestion.md §3.2 (stage 1,
// Raw Fetch, is the connector's Fetch channel itself, not a Stage in this
// slice - see pipeline.go's Pipeline doc comment).
var eightStageNames = []string{
	"format_parse", "field_mapping", "normalization",
	"validation", "dedup", "canonicalization", "persist_emit",
}

func TestPipeline_RunsStagesInOrder(t *testing.T) {
	p := &pipeline.Pipeline{Stages: passthroughStages(eightStageNames...)}

	tenantID := uuid.New()
	connectorID := uuid.New()
	rec := testconnector.NewRecord(tenantID, connectorID, []byte("payload"))

	outcome := p.ProcessOne(context.Background(), rec)

	if outcome.Quarantined || outcome.Dropped {
		t.Fatalf("expected the record to complete normally, got %+v", outcome)
	}
	if len(outcome.StageOrder) != len(eightStageNames) {
		t.Fatalf("expected %d stages to run, got %d: %v", len(eightStageNames), len(outcome.StageOrder), outcome.StageOrder)
	}
	for i, want := range eightStageNames {
		if outcome.StageOrder[i] != want {
			t.Errorf("stage %d: expected %q, got %q (full order: %v)", i, want, outcome.StageOrder[i], outcome.StageOrder)
		}
	}
}

func TestPipeline_QuarantineStopsProcessingButNotSiblings(t *testing.T) {
	stages := []pipeline.Stage{
		&fakeStage{name: "format_parse", result: pipeline.StageContinue},
		&fakeStage{name: "field_mapping", result: pipeline.StageContinue},
		&fakeStage{name: "validation", result: pipeline.StageQuarantine, err: errValidationFailed},
		&fakeStage{name: "dedup", result: pipeline.StageContinue}, // must NOT run for the quarantined record
	}

	quarantine := &fakeQuarantineSink{}
	p := &pipeline.Pipeline{Stages: stages, Quarantine: quarantine}

	tenantID := uuid.New()
	connectorID := uuid.New()
	rec := testconnector.NewRecord(tenantID, connectorID, []byte("bad-payload"))

	outcome := p.ProcessOne(context.Background(), rec)

	if !outcome.Quarantined {
		t.Fatal("expected the record to be quarantined")
	}
	wantOrder := []string{"format_parse", "field_mapping", "validation"}
	if len(outcome.StageOrder) != len(wantOrder) {
		t.Fatalf("expected processing to stop at validation (3 stages), got %v", outcome.StageOrder)
	}
	for i, want := range wantOrder {
		if outcome.StageOrder[i] != want {
			t.Errorf("stage %d: expected %q, got %q", i, want, outcome.StageOrder[i])
		}
	}

	quarantine.mu.Lock()
	defer quarantine.mu.Unlock()
	if len(quarantine.entries) != 1 {
		t.Fatalf("expected exactly 1 quarantine entry, got %d", len(quarantine.entries))
	}
	if quarantine.entries[0].stage != "validation" {
		t.Errorf("expected quarantine to record stage %q, got %q", "validation", quarantine.entries[0].stage)
	}
}

func TestPipeline_SiblingRecordsUnaffectedByOneQuarantine(t *testing.T) {
	// Every third record (index 1, 4, 7...) fails validation; the rest
	// must complete normally regardless - proving one bad record never
	// halts the pipeline (plans/docs/15-end-to-end-flows.md §15.5).
	var mu sync.Mutex
	callCount := 0

	stages := []pipeline.Stage{
		&fakeStage{name: "format_parse", result: pipeline.StageContinue},
		validationStageFailingEveryNth(&mu, &callCount, 3),
		&fakeStage{name: "canonicalization", result: pipeline.StageContinue},
	}

	quarantine := &fakeQuarantineSink{}
	outcomes := make([]pipeline.RecordOutcome, 0, 9)
	var outcomesMu sync.Mutex

	p := &pipeline.Pipeline{
		Stages:     stages,
		Quarantine: quarantine,
		OnRecordProcessed: func(o pipeline.RecordOutcome) {
			outcomesMu.Lock()
			defer outcomesMu.Unlock()
			outcomes = append(outcomes, o)
		},
	}

	tenantID := uuid.New()
	connectorID := uuid.New()
	records := make([]connector.RawRecord, 9)
	for i := range records {
		records[i] = testconnector.NewRecord(tenantID, connectorID, []byte("payload"))
	}
	conn := testconnector.New(records)

	if err := p.Run(context.Background(), conn, connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	outcomesMu.Lock()
	defer outcomesMu.Unlock()
	if len(outcomes) != 9 {
		t.Fatalf("expected 9 outcomes, got %d", len(outcomes))
	}

	quarantined := 0
	completed := 0
	for _, o := range outcomes {
		if o.Quarantined {
			quarantined++
		} else if !o.Dropped {
			completed++
			if len(o.StageOrder) != 3 {
				t.Errorf("expected a completed record to run all 3 stages, got %v", o.StageOrder)
			}
		}
	}
	if quarantined != 3 {
		t.Errorf("expected 3 quarantined records (every 3rd of 9), got %d", quarantined)
	}
	if completed != 6 {
		t.Errorf("expected 6 records to complete normally despite siblings being quarantined, got %d", completed)
	}
}

var errValidationFailed = fakeErr("validation failed")

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

type quarantineEntry struct {
	stage  string
	reason string
}

type fakeQuarantineSink struct {
	mu      sync.Mutex
	entries []quarantineEntry
}

func (f *fakeQuarantineSink) Quarantine(ctx context.Context, tenantID, connectorID uuid.UUID, stage, reason string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, quarantineEntry{stage: stage, reason: reason})
	return nil
}

// validationStageFailingEveryNth returns a Stage that quarantines every
// nth call (thread-safe counter) and passes through otherwise.
func validationStageFailingEveryNth(mu *sync.Mutex, counter *int, n int) pipeline.Stage {
	return &countingStage{mu: mu, counter: counter, n: n}
}

type countingStage struct {
	mu      *sync.Mutex
	counter *int
	n       int
}

func (s *countingStage) Name() string { return "validation" }
func (s *countingStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	s.mu.Lock()
	*s.counter++
	count := *s.counter
	s.mu.Unlock()

	if count%s.n == 0 {
		return pipeline.StageQuarantine, errValidationFailed
	}
	return pipeline.StageContinue, nil
}
