-- v2 Notes: optional owner title of a working note. No row means "Untitled note".
-- A separate table keeps the migration safe to re-run on PostgreSQL and SQLite.
CREATE TABLE IF NOT EXISTS note_titles (
 note_id TEXT PRIMARY KEY REFERENCES notes(note_id) ON DELETE CASCADE,
 title TEXT NOT NULL CHECK(title <> '' AND length(title) <= 200)
);
