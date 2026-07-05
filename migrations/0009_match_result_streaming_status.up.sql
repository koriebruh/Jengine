-- plans/task/core/19: additive enum expansion (expand-only, per the
-- task's own instruction - never drop/rename existing values).
-- AUTO_MATCHED_STREAMING: provisional, written by the streaming worker.
-- AUTO_MATCHED_CONFIRMED: final, written by the batch/streaming
-- reconciliation job once a streaming match is confirmed by the
-- authoritative batch pass. Plain AUTO_MATCHED (existing) is unchanged -
-- still what a batch-only match (no streaming counterpart to reconcile
-- against) gets.
ALTER TABLE match_results DROP CONSTRAINT match_results_status_check;
ALTER TABLE match_results ADD CONSTRAINT match_results_status_check
    CHECK (status IN ('AUTO_MATCHED', 'AUTO_MATCHED_STREAMING', 'AUTO_MATCHED_CONFIRMED', 'SUGGESTED', 'CONFIRMED', 'REJECTED'));

-- RECONCILIATION_VARIANCE: the lightweight-review Break created when a
-- streaming match and the authoritative batch pass disagree (different/
-- no counterpart, or a grouping streaming missed due to a late
-- counterpart or narrower window).
ALTER TABLE cases DROP CONSTRAINT cases_break_type_check;
ALTER TABLE cases ADD CONSTRAINT cases_break_type_check
    CHECK (break_type IN ('UNMATCHED', 'AMOUNT_MISMATCH', 'TIMING_DIFFERENCE', 'DUPLICATE', 'FX_VARIANCE', 'MISSING_COUNTERPARTY', 'RECONCILIATION_VARIANCE'));
