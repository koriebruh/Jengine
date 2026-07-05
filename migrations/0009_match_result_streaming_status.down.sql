ALTER TABLE match_results DROP CONSTRAINT match_results_status_check;
ALTER TABLE match_results ADD CONSTRAINT match_results_status_check
    CHECK (status IN ('AUTO_MATCHED', 'SUGGESTED', 'CONFIRMED', 'REJECTED'));

ALTER TABLE cases DROP CONSTRAINT cases_break_type_check;
ALTER TABLE cases ADD CONSTRAINT cases_break_type_check
    CHECK (break_type IN ('UNMATCHED', 'AMOUNT_MISMATCH', 'TIMING_DIFFERENCE', 'DUPLICATE', 'FX_VARIANCE', 'MISSING_COUNTERPARTY'));
