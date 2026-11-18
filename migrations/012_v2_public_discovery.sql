-- v2 public discovery. Both tables hold published data only: publish and republish
-- rebuild the rows of a note, unpublish removes them, and a note delete cascades.
-- SQLite cannot store a tsvector; tests/sqlite/012_v2_public_discovery.sql is its tested equivalent.
-- The tags of a note at its last publication. Later tag edits stay private until the next publication.
CREATE TABLE IF NOT EXISTS note_publication_tags (
 note_id TEXT NOT NULL REFERENCES note_publications(note_id) ON DELETE CASCADE,
 tag_id TEXT NOT NULL REFERENCES tags(tag_id) ON DELETE RESTRICT,
 PRIMARY KEY(note_id,tag_id)
);
CREATE INDEX IF NOT EXISTS note_publication_tags_by_tag ON note_publication_tags(tag_id,note_id);
-- The derived search index of a published note: the short summary and the distinct search terms of the
-- published title, summary, tags and published Typed text. It keeps no document body.
CREATE TABLE IF NOT EXISTS note_search (
 note_id TEXT PRIMARY KEY REFERENCES note_publications(note_id) ON DELETE CASCADE,
 summary TEXT NOT NULL CHECK(length(summary) <= 200),
 search_vector TSVECTOR NOT NULL
);
CREATE INDEX IF NOT EXISTS note_search_by_terms ON note_search USING GIN(search_vector);
