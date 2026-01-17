-- Add column to table if it doesn't exist (for schema migration)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = '{table}'
        AND column_name = '{columnName}'
    ) THEN
        ALTER TABLE {table} ADD COLUMN {columnName} {columnType};
    END IF;
END $$
