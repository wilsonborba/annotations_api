-- v2 durable optional Cortex content jobs. Output is saved as a private reviewable draft before
-- cleaning up the temporary Cortex session.
CREATE TABLE IF NOT EXISTS content_jobs (
 job_id TEXT PRIMARY KEY CHECK(job_id <> ''),
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 client_job_id TEXT NOT NULL CHECK(client_job_id <> ''),
 source_note_id TEXT REFERENCES notes(note_id) ON DELETE SET NULL,
 source_revision_id TEXT REFERENCES note_revisions(revision_id) ON DELETE SET NULL,
 kind TEXT NOT NULL CHECK(kind IN ('extraction','study_notes')),
 request_hash TEXT NOT NULL CHECK(length(request_hash) = 64),
 prompt_text TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL CHECK(status IN ('queued','uploading','running','completed','partial','cancelled','failed')),
 cortex_session_id TEXT,
 cortex_output_id TEXT,
 event_cursor BIGINT NOT NULL DEFAULT 0 CHECK(event_cursor >= 0),
 draft_document_id TEXT,
 draft_schema_name TEXT,
 draft_schema_version BIGINT,
 draft_saved BIGINT NOT NULL DEFAULT 0 CHECK(draft_saved IN (0,1)),
 cortex_cleaned BIGINT NOT NULL DEFAULT 0 CHECK(cortex_cleaned IN (0,1)),
 error_code TEXT,
 error_message TEXT,
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 updated_at BIGINT NOT NULL CHECK(updated_at >= 0),
 FOREIGN KEY(draft_schema_name,draft_schema_version) REFERENCES schema_versions(name,version) ON DELETE RESTRICT,
 UNIQUE(owner_id,client_job_id)
);
CREATE INDEX IF NOT EXISTS content_jobs_by_owner ON content_jobs(owner_id,created_at);
CREATE TABLE IF NOT EXISTS content_job_files (
 job_id TEXT NOT NULL REFERENCES content_jobs(job_id) ON DELETE CASCADE,
 file_id TEXT NOT NULL REFERENCES files(file_id) ON DELETE RESTRICT,
 cortex_file_id TEXT,
 position BIGINT NOT NULL CHECK(position >= 0),
 PRIMARY KEY(job_id,file_id)
);
CREATE TABLE IF NOT EXISTS content_job_events (
 job_id TEXT NOT NULL REFERENCES content_jobs(job_id) ON DELETE CASCADE,
 seq BIGINT NOT NULL CHECK(seq > 0),
 stage TEXT NOT NULL CHECK(stage IN ('queued','uploading','running','completed','partial','cancelled','failed')),
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 PRIMARY KEY(job_id,seq)
);
