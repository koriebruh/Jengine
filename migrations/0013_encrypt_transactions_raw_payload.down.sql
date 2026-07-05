ALTER TABLE transactions ALTER COLUMN raw_payload TYPE jsonb
  USING '{}'::jsonb;
ALTER TABLE transactions ALTER COLUMN raw_payload SET DEFAULT '{}'::jsonb;
ALTER TABLE transactions ALTER COLUMN raw_payload SET NOT NULL;
