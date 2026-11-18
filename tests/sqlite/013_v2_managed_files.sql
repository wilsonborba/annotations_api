-- v2 managed files: the file name that POST /me/files sends, the value lists of files.purpose and
-- file_references.reference_kind, and the durable FSM cleanup queue. File bytes stay in FSM.
-- Each ready file owns one FSM media item; FSM keeps one blob per SHA-256 and counts its items.
-- SQLite equivalent of migrations/013_v2_managed_files.sql. Only the value lists differ: triggers replace the CHECKs.
ALTER TABLE files ADD COLUMN filename TEXT NOT NULL DEFAULT 'file' CHECK(filename <> '' AND length(filename) <= 255);
CREATE TRIGGER IF NOT EXISTS files_purpose_insert BEFORE INSERT ON files WHEN NEW.purpose NOT IN ('attachment','transcription','content_job') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: files_purpose_values'); END;
CREATE TRIGGER IF NOT EXISTS files_purpose_update BEFORE UPDATE OF purpose ON files WHEN NEW.purpose NOT IN ('attachment','transcription','content_job') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: files_purpose_values'); END;
CREATE TRIGGER IF NOT EXISTS file_references_kind_insert BEFORE INSERT ON file_references WHEN NEW.reference_kind NOT IN ('note_revision','pending_operation','transcription','content_job') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: file_references_kind_values'); END;
CREATE TRIGGER IF NOT EXISTS file_references_kind_update BEFORE UPDATE OF reference_kind ON file_references WHEN NEW.reference_kind NOT IN ('note_revision','pending_operation','transcription','content_job') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: file_references_kind_values'); END;
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
