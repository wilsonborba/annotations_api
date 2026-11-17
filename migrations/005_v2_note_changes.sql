-- v2 sync change feed. Append-only. Rows keep no foreign key to notes so delete notices survive the note row.
-- SQLite cannot run the identity column below; tests/sqlite/005_v2_note_changes.sql is its tested equivalent.
CREATE TABLE IF NOT EXISTS note_changes (
 sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 note_id TEXT NOT NULL,
 change_kind TEXT NOT NULL CHECK(change_kind IN ('upsert','delete')),
 changed_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS note_changes_by_owner ON note_changes(owner_id,sequence);
