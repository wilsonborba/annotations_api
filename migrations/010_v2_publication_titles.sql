-- v2 Notes: the title of a note at its last publication. Public reads use this
-- snapshot, so a later working-copy title change never reaches public readers.
-- No row means the published note has no title. Unpublish removes the row.
CREATE TABLE IF NOT EXISTS note_publication_titles (
 note_id TEXT PRIMARY KEY REFERENCES note_publications(note_id) ON DELETE CASCADE,
 title TEXT NOT NULL CHECK(title <> '' AND length(title) <= 200)
);
