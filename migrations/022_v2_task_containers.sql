-- v2 Plane Slim per-owner private task containers for the Annotations app.
CREATE TABLE IF NOT EXISTS owner_plane_containers (
 owner_id TEXT PRIMARY KEY REFERENCES owners(id) ON DELETE CASCADE,
 plane_project_id TEXT NOT NULL CHECK(plane_project_id <> ''),
 open_state_id TEXT NOT NULL CHECK(open_state_id <> ''),
 completed_state_id TEXT NOT NULL CHECK(completed_state_id <> ''),
 created_at BIGINT NOT NULL CHECK(created_at >= 0)
);
