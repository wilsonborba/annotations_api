-- v2 note save pipeline: value list for pending_operations.state and the rows of each note save operation.
-- started: running; CouchDB documents may exist, nothing is visible. done: the PostgreSQL transaction committed.
-- abandoned: stopped before commit, its documents still need removal. failed: its documents are removed.
-- SQLite equivalent of migrations/008_v2_save_operations.sql. Only the state value list differs: triggers replace the CHECK.
CREATE TRIGGER IF NOT EXISTS pending_operations_state_insert BEFORE INSERT ON pending_operations WHEN NEW.state NOT IN ('started','abandoned','done','failed') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: pending_operations_state_values'); END;
CREATE TRIGGER IF NOT EXISTS pending_operations_state_update BEFORE UPDATE OF state ON pending_operations WHEN NEW.state NOT IN ('started','abandoned','done','failed') BEGIN SELECT RAISE(ABORT,'CHECK constraint failed: pending_operations_state_values'); END;
CREATE TABLE IF NOT EXISTS note_save_operations (
 operation_id TEXT PRIMARY KEY REFERENCES pending_operations(operation_id) ON DELETE CASCADE,
 note_id TEXT NOT NULL CHECK(note_id <> ''),
 request_hash TEXT NOT NULL CHECK(length(request_hash) = 64),
 result_revision BIGINT CHECK(result_revision IS NULL OR result_revision > 0),
 attempts BIGINT NOT NULL DEFAULT 0 CHECK(attempts >= 0),
 updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS note_save_operations_by_time ON note_save_operations(updated_at);
CREATE TABLE IF NOT EXISTS note_save_revisions (
 operation_id TEXT NOT NULL REFERENCES note_save_operations(operation_id) ON DELETE CASCADE,
 representation TEXT NOT NULL CHECK(representation IN ('original','typed')),
 revision_id TEXT NOT NULL UNIQUE,
 document_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(operation_id,representation)
);

-- Durable document deletion after an unreferenced revision is removed.
CREATE TABLE IF NOT EXISTS note_revision_cleanup (
 document_id TEXT PRIMARY KEY,
 next_attempt_at BIGINT NOT NULL DEFAULT 0,
 attempts BIGINT NOT NULL DEFAULT 0 CHECK(attempts >= 0),
 created_at BIGINT NOT NULL
);
