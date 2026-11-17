-- v2 managed file references and pending multi-store save operations. File bytes stay in FSM.
CREATE TABLE IF NOT EXISTS files (
 file_id TEXT PRIMARY KEY,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 fsm_reference TEXT,
 sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
 size_bytes BIGINT NOT NULL CHECK(size_bytes >= 0),
 content_type TEXT NOT NULL CHECK(content_type <> ''),
 purpose TEXT NOT NULL CHECK(purpose <> ''),
 status TEXT NOT NULL DEFAULT 'reserved' CHECK(status IN ('reserved','uploading','ready','failed')),
 created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 CHECK(status <> 'ready' OR fsm_reference IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS files_by_owner ON files(owner_id,status);
CREATE TABLE IF NOT EXISTS file_references (
 file_id TEXT NOT NULL REFERENCES files(file_id) ON DELETE RESTRICT,
 reference_kind TEXT NOT NULL CHECK(reference_kind <> ''),
 reference_id TEXT NOT NULL CHECK(reference_id <> ''),
 PRIMARY KEY(file_id,reference_kind,reference_id)
);
CREATE INDEX IF NOT EXISTS file_references_by_target ON file_references(reference_kind,reference_id);
CREATE TABLE IF NOT EXISTS pending_operations (
 operation_id TEXT PRIMARY KEY,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 state TEXT NOT NULL CHECK(state <> ''),
 created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS pending_operations_by_state ON pending_operations(state,created_at);
