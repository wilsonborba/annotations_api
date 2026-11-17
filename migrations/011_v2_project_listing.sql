-- v2 owner project list (/me/projects): newest change first with a keyset cursor.
CREATE INDEX IF NOT EXISTS projects_by_owner_updated ON projects(owner_id,updated_at,project_id);
