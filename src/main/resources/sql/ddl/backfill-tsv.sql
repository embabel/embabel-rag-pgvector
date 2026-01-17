-- Backfill tsv for any existing rows that have NULL tsv
-- This triggers the tsv trigger function on existing rows
UPDATE {table}
SET text = text
WHERE tsv IS NULL AND text IS NOT NULL
