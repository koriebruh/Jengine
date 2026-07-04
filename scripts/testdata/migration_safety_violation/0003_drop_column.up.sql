-- Deliberately non-compliant: bare DROP COLUMN with no deprecation marker.
-- scripts/check-migration-safety.sh must flag this.
ALTER TABLE accounts DROP COLUMN nickname;
