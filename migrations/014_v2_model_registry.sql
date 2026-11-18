-- Metadata only. Model bytes and authoritative training data stay outside this database.
CREATE TABLE IF NOT EXISTS model_releases (
 version TEXT PRIMARY KEY CHECK(length(version) BETWEEN 1 AND 128),
 status TEXT NOT NULL CHECK(status IN ('draft','active','retired')),
 code_revision TEXT NOT NULL CHECK(code_revision <> ''),
 dataset_snapshot_id TEXT NOT NULL CHECK(dataset_snapshot_id <> ''),
 source_checkpoint TEXT NOT NULL CHECK(source_checkpoint <> ''),
 created_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS model_one_active_release ON model_releases(status) WHERE status = 'active';
CREATE TABLE IF NOT EXISTS model_artifacts (
 version TEXT NOT NULL REFERENCES model_releases(version),
 platform TEXT NOT NULL CHECK(platform IN ('android','ios','macos','linux')),
 fsm_reference TEXT NOT NULL CHECK(fsm_reference <> ''),
 size BIGINT NOT NULL CHECK(size > 0),
 sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 = lower(sha256)),
 PRIMARY KEY(version,platform)
);
CREATE TABLE IF NOT EXISTS model_languages (
 version TEXT NOT NULL REFERENCES model_releases(version),
 language TEXT NOT NULL CHECK(language IN ('en','pt-BR','th')),
 PRIMARY KEY(version,language)
);
CREATE TABLE IF NOT EXISTS model_metrics (
 version TEXT NOT NULL,
 language TEXT NOT NULL,
 name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 64),
 value DOUBLE PRECISION NOT NULL CHECK(value >= 0),
 PRIMARY KEY(version,language,name),
 FOREIGN KEY(version,language) REFERENCES model_languages(version,language)
);
CREATE TABLE IF NOT EXISTS model_capabilities (
 version TEXT NOT NULL REFERENCES model_releases(version),
 capability TEXT NOT NULL CHECK(length(capability) BETWEEN 1 AND 64),
 PRIMARY KEY(version,capability)
);
-- One row serializes promotion, retaining the previous valid release for rollback.
CREATE TABLE IF NOT EXISTS model_default (
 id BIGINT PRIMARY KEY CHECK(id = 1),
 current_version TEXT REFERENCES model_releases(version),
 previous_version TEXT REFERENCES model_releases(version),
 CHECK(previous_version IS NULL OR current_version IS NOT NULL),
 CHECK(previous_version IS NULL OR previous_version <> current_version)
);
INSERT INTO model_default(id) VALUES(1) ON CONFLICT(id) DO NOTHING;
