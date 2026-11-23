CREATE TABLE source_watermark (
    source_id uuid PRIMARY KEY REFERENCES source_config(id) ON DELETE RESTRICT,
    watermark timestamptz,
    tie_breaker jsonb,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    publication_uri text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((watermark IS NULL) = (tie_breaker IS NULL)),
    CHECK (publication_uri IS NULL OR btrim(publication_uri) <> '')
);

CREATE TRIGGER source_watermark_touch BEFORE UPDATE ON source_watermark
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
