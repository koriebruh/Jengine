-- Compliant expand-only migration - scripts/check-migration-safety.sh must NOT flag this.
ALTER TABLE accounts ADD COLUMN nickname text;
