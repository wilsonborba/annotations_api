-- Scoped created-for-note vs linked Reports item accounting and retryable cascade cleanup (#37).
CREATE TABLE IF NOT EXISTS note_report_references (
    note_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    report_item_id TEXT NOT NULL,
    relation_kind TEXT NOT NULL CHECK (relation_kind IN ('created_for_note', 'linked')),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (note_id, report_item_id)
);

CREATE INDEX IF NOT EXISTS idx_note_report_refs_item
    ON note_report_references (report_item_id, relation_kind);

CREATE TABLE IF NOT EXISTS note_report_cleanup (
    note_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    report_item_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'deleted', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (note_id, report_item_id)
);
