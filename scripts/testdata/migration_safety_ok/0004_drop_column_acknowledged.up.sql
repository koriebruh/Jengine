-- A breaking statement below, but with the required override marker within
-- 3 lines above it - scripts/check-migration-safety.sh must NOT flag this.
-- expand-contract: deprecate-step-ref 0002
ALTER TABLE accounts DROP COLUMN nickname;
