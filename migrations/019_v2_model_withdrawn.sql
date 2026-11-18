-- v2 model releases: allow the withdrawn status so rolled-back or bad releases are never served.
-- SQLite cannot alter an inline CHECK constraint; tests/sqlite/019_v2_model_withdrawn.sql rebuilds the table.
ALTER TABLE model_releases DROP CONSTRAINT IF EXISTS model_releases_status_check;
ALTER TABLE model_releases DROP CONSTRAINT IF EXISTS model_releases_status_values;
ALTER TABLE model_releases ADD CONSTRAINT model_releases_status_values CHECK(status IN ('draft','active','retired','withdrawn'));
