-- Account-bound logical model downloads. One row per (owner, version, platform): retries
-- reuse it. The row points at the shared release artifact; no per-user copy of the bytes.
CREATE TABLE IF NOT EXISTS model_downloads (
 download_id TEXT PRIMARY KEY CHECK(length(download_id) BETWEEN 1 AND 64),
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 version TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ('android','ios','macos','linux')),
 created_at BIGINT NOT NULL CHECK(created_at >= 0),
 UNIQUE(owner_id,version,platform),
 FOREIGN KEY(version,platform) REFERENCES model_artifacts(version,platform)
);
