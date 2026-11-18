-- v2 model releases: allow the withdrawn status so rolled-back or bad releases are never served.
-- SQLite equivalent of migrations/019_v2_model_withdrawn.sql. Rebuilds model_releases to widen its CHECK.
PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS model_releases_next;
CREATE TABLE model_releases_next (
 version TEXT PRIMARY KEY CHECK(length(version) BETWEEN 1 AND 128),
 status TEXT NOT NULL CHECK(status IN ('draft','active','retired','withdrawn')),
 code_revision TEXT NOT NULL CHECK(code_revision <> ''),
 dataset_snapshot_id TEXT NOT NULL CHECK(dataset_snapshot_id <> ''),
 source_checkpoint TEXT NOT NULL CHECK(source_checkpoint <> ''),
 created_at BIGINT NOT NULL
);
INSERT INTO model_releases_next(version,status,code_revision,dataset_snapshot_id,source_checkpoint,created_at)
 SELECT version,status,code_revision,dataset_snapshot_id,source_checkpoint,created_at FROM model_releases;
DROP TABLE model_releases;
ALTER TABLE model_releases_next RENAME TO model_releases;
CREATE UNIQUE INDEX IF NOT EXISTS model_one_active_release ON model_releases(status) WHERE status = 'active';
PRAGMA foreign_keys=ON;
