-- v2 managed files: the file name that POST /me/files sends, the value lists of files.purpose and
-- file_references.reference_kind, and the durable FSM cleanup queue. File bytes stay in FSM.
-- Each ready file owns one FSM media item; FSM keeps one blob per SHA-256 and counts its items.
-- SQLite cannot add a CHECK to an existing table; tests/sqlite/013_v2_managed_files.sql uses triggers instead.
ALTER TABLE files ADD COLUMN IF NOT EXISTS filename TEXT NOT NULL DEFAULT 'file';
ALTER TABLE files DROP CONSTRAINT IF EXISTS files_filename_values;
ALTER TABLE files ADD CONSTRAINT files_filename_values CHECK(filename <> '' AND length(filename) <= 255);
ALTER TABLE files DROP CONSTRAINT IF EXISTS files_purpose_values;
ALTER TABLE files ADD CONSTRAINT files_purpose_values CHECK(purpose IN ('attachment','transcription','content_job'));
ALTER TABLE file_references DROP CONSTRAINT IF EXISTS file_references_kind_values;
ALTER TABLE file_references ADD CONSTRAINT file_references_kind_values CHECK(reference_kind IN ('note_revision','pending_operation','transcription','content_job'));
CREATE INDEX IF NOT EXISTS files_by_fsm_reference ON files(fsm_reference);
CREATE INDEX IF NOT EXISTS files_by_status ON files(status,updated_at);
-- FSM media items to delete after their file row is gone. The task retries a failed delete a bounded number of times.
CREATE TABLE IF NOT EXISTS file_cleanup (
 fsm_reference TEXT PRIMARY KEY CHECK(fsm_reference <> ''),
 next_attempt_at BIGINT NOT NULL DEFAULT 0,
 attempts BIGINT NOT NULL DEFAULT 0 CHECK(attempts >= 0),
 created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS file_cleanup_by_time ON file_cleanup(next_attempt_at);
