-- Create trigger for automatic tsvector maintenance on insert/update
CREATE TRIGGER {table}_tsv_update
BEFORE INSERT OR UPDATE ON {table}
FOR EACH ROW
EXECUTE FUNCTION {table}_tsv_trigger()
