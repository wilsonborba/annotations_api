-- v2 handwritten study links: each study note can link to one published typed source post.
-- Deleting or unpublishing the source never deletes or changes the study note.
CREATE TABLE IF NOT EXISTS note_study_sources (
 note_id TEXT PRIMARY KEY REFERENCES notes(note_id) ON DELETE CASCADE,
 source_note_id TEXT REFERENCES notes(note_id) ON DELETE SET NULL,
 source_title TEXT NOT NULL,
 source_author_id TEXT,
 source_author_name TEXT NOT NULL DEFAULT '',
 created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS note_study_sources_by_source ON note_study_sources(source_note_id);
