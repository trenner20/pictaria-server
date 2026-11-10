CREATE TABLE IF NOT EXISTS caption_index (
  asset_id text PRIMARY KEY,
  caption text,
  short_caption text,
  document tsvector GENERATED ALWAYS AS
    (to_tsvector('simple', coalesce(caption, '') || ' ' || coalesce(short_caption, ''))) STORED
);
CREATE INDEX IF NOT EXISTS idx_caption_index_document ON caption_index USING gin(document);
CREATE OR REPLACE VIEW caption_vocab AS
  SELECT term, count(*) AS doc
  FROM caption_index CROSS JOIN LATERAL unnest(tsvector_to_array(document)) AS term
  GROUP BY term;
