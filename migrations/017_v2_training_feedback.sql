-- Correction feedback of transcription runs, kept as untrusted training input.
-- Each row is one event of one run of its account. The validated event is a CouchDB
-- document (schema training-feedback); this file stores its id only.
-- status: received (stored, not reviewed), flagged (a second copy of an earlier example
-- of the same run; excluded from export unless asked) or approved_for_training. No code
-- path sets approved_for_training automatically.
-- Rows stay until an administrator removes them with the server command line.
-- source_note_id is a plain copy without a foreign key: removing the note keeps the rows.
CREATE TABLE IF NOT EXISTS training_feedback (
 feedback_id TEXT PRIMARY KEY CHECK(length(feedback_id) BETWEEN 1 AND 64),
 run_id TEXT NOT NULL REFERENCES transcription_runs(run_id) ON DELETE RESTRICT,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 event_id TEXT NOT NULL CHECK(length(event_id) BETWEEN 1 AND 128),
 request_hash TEXT NOT NULL CHECK(length(request_hash) = 64),
 content_hash TEXT NOT NULL CHECK(length(content_hash) = 64),
 segment_id TEXT NOT NULL CHECK(length(segment_id) BETWEEN 1 AND 128),
 stage TEXT NOT NULL CHECK(stage IN ('intermediate','accepted')),
 status TEXT NOT NULL CHECK(status IN ('received','flagged','approved_for_training')),
 source_note_id TEXT CHECK(source_note_id IS NULL OR length(source_note_id) BETWEEN 1 AND 128),
 document_id TEXT NOT NULL UNIQUE CHECK(document_id <> ''),
 schema_name TEXT NOT NULL CHECK(schema_name = 'training-feedback'),
 schema_version BIGINT NOT NULL,
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 UNIQUE(run_id,event_id),
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS training_feedback_by_content ON training_feedback(run_id,content_hash);
CREATE INDEX IF NOT EXISTS training_feedback_by_owner ON training_feedback(owner_id);
CREATE INDEX IF NOT EXISTS training_feedback_by_note ON training_feedback(source_note_id);
