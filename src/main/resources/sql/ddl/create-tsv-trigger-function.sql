-- Create trigger function for automatic tsvector and tokens maintenance
CREATE OR REPLACE FUNCTION {table}_tsv_trigger()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tsv := to_tsvector('english', COALESCE(NEW.text, ''));
    NEW.clean_text := regexp_replace(LOWER(COALESCE(NEW.text, '')), '[^a-z0-9\s]', '', 'g');
    NEW.tokens := regexp_split_to_array(NEW.clean_text, '\s+');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
