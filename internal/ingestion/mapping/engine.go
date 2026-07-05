package mapping

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/platform/tokenization"
)

// MappingSpecLookup is the surface MappingEngine needs -
// internal/storage/postgres.MappingSpecRepo satisfies it structurally.
type MappingSpecLookup interface {
	GetActive(ctx context.Context, tenantID uuid.UUID, sourceFormat string) (domain.MappingSpec, error)
}

// TxRunner wraps fn in a transaction scoped to tenantID. Like every other
// Stage in this pipeline that needs DB access (see
// postgres.PersistEmitStage, connector/csvupload.TxRunner,
// connector/sftp.TxRunner), Process is called directly by
// pipeline.Pipeline.Run outside any ambient transaction, so a Stage that
// itself needs one must open it. Satisfied by a thin closure around
// postgres.WithTx in production; tests can pass a pass-through that just
// calls fn directly.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// specCacheTTL mirrors plans/docs/15-end-to-end-flows.md §15.3's rule-
// cache-with-short-TTL pattern for MatchRules, reused here for
// MappingSpecs (plans/task/core/08 Implementation Notes).
const specCacheTTL = 30 * time.Second

type cachedSpec struct {
	spec      MappingSpec
	expiresAt time.Time
}

// MappingEngine implements pipeline stage 3 (Field Mapping,
// plans/task/core/08): loads a tenant's active MappingSpec for the
// record's source format, applies each FieldMapping's transform chain
// left-to-right, and writes results into rec.MappedFields keyed by their
// canonical target path (e.g. "transaction.amount"). Stateless aside from
// the short-TTL spec cache, safe for concurrent use across the pipeline's
// bounded worker pool (plans/task/core/08 Implementation Notes).
type MappingEngine struct {
	Specs    MappingSpecLookup
	TxRunner TxRunner
	// Tokenizer backs the `tokenize` transform (plans/task/core/23) - nil
	// is fine for tenants/specs that never use it; a spec that DOES use
	// `tokenize` with a nil Tokenizer fails loudly per-record (see
	// tokenize's own doc comment), not silently.
	Tokenizer tokenization.TokenizationService

	cacheMu sync.RWMutex
	cache   map[string]cachedSpec
}

func NewEngine(specs MappingSpecLookup, txRunner TxRunner) *MappingEngine {
	return &MappingEngine{Specs: specs, TxRunner: txRunner, cache: make(map[string]cachedSpec)}
}

func (e *MappingEngine) Name() string { return "field_mapping" }

func (e *MappingEngine) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	tenantID := rec.Raw.TenantID
	sourceFormat := rec.Raw.SourceFormat

	spec, err := e.loadSpec(ctx, tenantID, sourceFormat)
	if err != nil {
		return pipeline.StageQuarantine, fmt.Errorf("field_mapping: load spec: %w", err)
	}

	mapped := make(map[string]any, len(spec.Mappings))
	for _, fm := range spec.Mappings {
		value, err := resolveFieldPath(rec.ParsedFields, fm.Source)
		if err != nil {
			return pipeline.StageQuarantine, fmt.Errorf("field_mapping: target %q: %w", fm.Target, err)
		}

		for _, callRaw := range fm.Transform {
			call, err := ParseTransformCall(callRaw)
			if err != nil {
				return pipeline.StageQuarantine, fmt.Errorf("field_mapping: target %q: %w", fm.Target, err)
			}
			fn, ok := Get(call.Name)
			if !ok {
				return pipeline.StageQuarantine, fmt.Errorf("field_mapping: target %q: unknown transform %q", fm.Target, call.Name)
			}
			var args []string
			if call.HasArg {
				args = []string{call.Arg}
			}
			value, err = fn(TransformContext{
				Record: rec.ParsedFields, Ctx: ctx, TenantID: tenantID.String(),
				Tokenizer: e.Tokenizer, TargetField: fm.Target,
			}, value, args...)
			if err != nil {
				return pipeline.StageQuarantine, fmt.Errorf("field_mapping: target %q: transform %q: %w", fm.Target, call.Name, err)
			}
		}

		mapped[fm.Target] = value
	}

	rec.MappedFields = mapped
	return pipeline.StageContinue, nil
}

func (e *MappingEngine) loadSpec(ctx context.Context, tenantID uuid.UUID, sourceFormat string) (MappingSpec, error) {
	cacheKey := tenantID.String() + "|" + sourceFormat

	e.cacheMu.RLock()
	cached, ok := e.cache[cacheKey]
	e.cacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.spec, nil
	}

	var record domain.MappingSpec
	err := e.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		record, err = e.Specs.GetActive(ctx, tenantID, sourceFormat)
		return err
	})
	if err != nil {
		return MappingSpec{}, err
	}
	spec, err := ParseSpecYAML(record.Spec)
	if err != nil {
		// mapping_specs.spec is stored as jsonb - JSON is a YAML subset,
		// so the YAML parser handles it directly.
		return MappingSpec{}, fmt.Errorf("parse stored spec: %w", err)
	}

	e.cacheMu.Lock()
	e.cache[cacheKey] = cachedSpec{spec: spec, expiresAt: time.Now().Add(specCacheTTL)}
	e.cacheMu.Unlock()

	return spec, nil
}

var _ pipeline.Stage = (*MappingEngine)(nil)
