-- Server-owned transcription runs. A run belongs to one account and one of that account's model
-- downloads. Its source is a saved Original revision or a client source hash of a note that is
-- not synced. Input ink and result content are CouchDB documents validated against a published
-- schema version; this file stores their ids only. Input files use file_references
-- (reference_kind 'transcription', reference_id = run_id), which also protects them from release.
-- Run checks give traceable submissions. They are not proof of honest local inference.
CREATE UNIQUE INDEX IF NOT EXISTS model_downloads_by_id_owner ON model_downloads(download_id,owner_id);
CREATE TABLE IF NOT EXISTS transcription_runs (
 run_id TEXT PRIMARY KEY CHECK(length(run_id) BETWEEN 1 AND 64),
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 download_id TEXT NOT NULL,
 language TEXT NOT NULL CHECK(language IN ('en','pt-BR','th')),
 mode TEXT NOT NULL CHECK(mode IN ('local','cloud')),
 status TEXT NOT NULL CHECK(status IN ('registered','completed','cancelled')),
 source_kind TEXT NOT NULL CHECK(source_kind IN ('revision','hash')),
 -- Set to NULL only when the note is deleted; the run and its training source stay.
 source_revision_id TEXT REFERENCES note_revisions(revision_id) ON DELETE SET NULL,
 source_hash TEXT CHECK(source_hash IS NULL OR length(source_hash) BETWEEN 1 AND 128),
 request_hash TEXT NOT NULL CHECK(length(request_hash) = 64),
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 updated_at BIGINT NOT NULL CHECK(updated_at >= created_at),
 CHECK((source_kind = 'hash' AND source_hash IS NOT NULL AND source_revision_id IS NULL) OR (source_kind = 'revision' AND source_hash IS NULL)),
 FOREIGN KEY(download_id,owner_id) REFERENCES model_downloads(download_id,owner_id) ON DELETE RESTRICT
);
-- One live run per identical request of an account: a retry or a reopen reuses it.
CREATE UNIQUE INDEX IF NOT EXISTS transcription_runs_reuse ON transcription_runs(owner_id,request_hash) WHERE status <> 'cancelled';
CREATE INDEX IF NOT EXISTS transcription_runs_by_source ON transcription_runs(source_revision_id);
-- The selected ink of a run (a note-original document with stroke and shape items only).
CREATE TABLE IF NOT EXISTS transcription_run_inks (
 run_id TEXT PRIMARY KEY REFERENCES transcription_runs(run_id) ON DELETE CASCADE,
 document_id TEXT NOT NULL UNIQUE CHECK(document_id <> ''),
 schema_name TEXT NOT NULL CHECK(schema_name = 'note-original'),
 schema_version BIGINT NOT NULL,
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version) ON DELETE RESTRICT
);
-- The one accepted result of a run: a note-typed draft document. Never published by the run.
CREATE TABLE IF NOT EXISTS transcription_results (
 run_id TEXT PRIMARY KEY REFERENCES transcription_runs(run_id) ON DELETE CASCADE,
 event_id TEXT NOT NULL CHECK(length(event_id) BETWEEN 1 AND 128),
 request_hash TEXT NOT NULL CHECK(length(request_hash) = 64),
 document_id TEXT NOT NULL UNIQUE CHECK(document_id <> ''),
 schema_name TEXT NOT NULL CHECK(schema_name = 'note-typed'),
 schema_version BIGINT NOT NULL,
 runtime_name TEXT NOT NULL CHECK(length(runtime_name) BETWEEN 1 AND 64),
 runtime_version TEXT NOT NULL CHECK(length(runtime_version) BETWEEN 1 AND 64),
 elapsed_ms BIGINT NOT NULL CHECK(elapsed_ms >= 0),
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version) ON DELETE RESTRICT
);
-- Real server-side stages in order. seq is the SSE event id and the after cursor.
CREATE TABLE IF NOT EXISTS transcription_run_events (
 run_id TEXT NOT NULL REFERENCES transcription_runs(run_id) ON DELETE CASCADE,
 seq BIGINT NOT NULL CHECK(seq > 0),
 stage TEXT NOT NULL CHECK(stage IN ('registered','result_received','cancelled')),
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 PRIMARY KEY(run_id,seq)
);
