CREATE TABLE IF NOT EXISTS note_report_references (
    note_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    report_item_id TEXT NOT NULL,
    relation_kind TEXT NOT NULL CHECK (relation_kind IN ('created_for_note', 'linked')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (note_id, report_item_id)
);

CREATE TABLE IF NOT EXISTS note_report_create_receipts (
    note_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    operation_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    project TEXT NOT NULL DEFAULT '',
    request_digest TEXT NOT NULL DEFAULT '',
    report_item_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending', 'recorded', 'rejected')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (note_id, owner_id, operation_key)
);

CREATE INDEX IF NOT EXISTS idx_note_report_refs_item
    ON note_report_references (report_item_id, relation_kind);

CREATE UNIQUE INDEX IF NOT EXISTS idx_note_report_refs_single_creator
    ON note_report_references (report_item_id)
    WHERE relation_kind = 'created_for_note';

CREATE TABLE IF NOT EXISTS note_report_cleanup (
    note_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    report_item_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'deleted', 'failed')),
    confirm_reused INTEGER NOT NULL DEFAULT 0 CHECK (confirm_reused IN (0, 1)),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (note_id, report_item_id)
);
