-- v2 organization: projects, memberships and optional user-added tags. No default tag.
CREATE TABLE IF NOT EXISTS projects (
 project_id TEXT PRIMARY KEY,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 name TEXT NOT NULL,
 normalized_name TEXT NOT NULL CHECK(normalized_name <> ''),
 visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility IN ('private','public')),
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0),
 created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 UNIQUE(owner_id,normalized_name),
 UNIQUE(project_id,owner_id)
);
CREATE INDEX IF NOT EXISTS projects_by_owner ON projects(owner_id,visibility,updated_at);
CREATE TABLE IF NOT EXISTS project_notes (
 project_id TEXT NOT NULL,
 note_id TEXT NOT NULL,
 owner_id TEXT NOT NULL,
 PRIMARY KEY(project_id,note_id),
 FOREIGN KEY(project_id,owner_id) REFERENCES projects(project_id,owner_id) ON DELETE CASCADE,
 FOREIGN KEY(note_id,owner_id) REFERENCES notes(note_id,owner_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS project_notes_by_note ON project_notes(note_id);
CREATE TABLE IF NOT EXISTS tags (
 tag_id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 normalized_name TEXT NOT NULL UNIQUE CHECK(normalized_name <> '')
);
CREATE TABLE IF NOT EXISTS note_tags (
 note_id TEXT NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
 tag_id TEXT NOT NULL REFERENCES tags(tag_id) ON DELETE RESTRICT,
 PRIMARY KEY(note_id,tag_id)
);
CREATE INDEX IF NOT EXISTS note_tags_by_tag ON note_tags(tag_id,note_id);
