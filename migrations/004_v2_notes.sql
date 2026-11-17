-- v2 Notes: identity, revisions, current pointers and publication. No document body is stored here.
CREATE TABLE IF NOT EXISTS authors (
 author_id TEXT PRIMARY KEY,
 owner_id TEXT NOT NULL UNIQUE REFERENCES owners(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS notes (
 note_id TEXT PRIMARY KEY,
 owner_id TEXT NOT NULL REFERENCES owners(id) ON DELETE RESTRICT,
 client_note_id TEXT NOT NULL CHECK(client_note_id <> ''),
 main_representation TEXT NOT NULL CHECK(main_representation IN ('original','typed')),
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0),
 created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 UNIQUE(owner_id,client_note_id),
 UNIQUE(note_id,owner_id)
);
CREATE INDEX IF NOT EXISTS notes_by_owner ON notes(owner_id,updated_at);
CREATE TABLE IF NOT EXISTS note_revisions (
 revision_id TEXT PRIMARY KEY,
 note_id TEXT NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
 representation TEXT NOT NULL CHECK(representation IN ('original','typed')),
 document_id TEXT NOT NULL UNIQUE,
 schema_name TEXT NOT NULL, schema_version BIGINT NOT NULL,
 content_hash TEXT NOT NULL CHECK(content_hash <> ''),
 source_revision_id TEXT,
 created_at BIGINT NOT NULL,
 UNIQUE(revision_id,note_id,representation),
 UNIQUE(revision_id,note_id),
 CHECK(source_revision_id IS NULL OR representation = 'typed'),
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version) ON DELETE RESTRICT,
 FOREIGN KEY(source_revision_id,note_id) REFERENCES note_revisions(revision_id,note_id) ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS note_revisions_by_note ON note_revisions(note_id,representation,created_at);
CREATE INDEX IF NOT EXISTS note_revisions_by_source ON note_revisions(source_revision_id);
CREATE TABLE IF NOT EXISTS note_current_revisions (
 note_id TEXT NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
 representation TEXT NOT NULL CHECK(representation IN ('original','typed')),
 revision_id TEXT NOT NULL,
 PRIMARY KEY(note_id,representation),
 FOREIGN KEY(revision_id,note_id,representation) REFERENCES note_revisions(revision_id,note_id,representation) ON DELETE NO ACTION
);
CREATE TABLE IF NOT EXISTS note_publications (
 note_id TEXT PRIMARY KEY REFERENCES notes(note_id) ON DELETE CASCADE,
 language_code TEXT NOT NULL REFERENCES languages(code) ON DELETE RESTRICT,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision > 0),
 updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS note_publications_by_time ON note_publications(updated_at);
CREATE INDEX IF NOT EXISTS note_publications_by_language ON note_publications(language_code,updated_at);
CREATE TABLE IF NOT EXISTS note_published_representations (
 note_id TEXT NOT NULL REFERENCES note_publications(note_id) ON DELETE CASCADE,
 representation TEXT NOT NULL CHECK(representation IN ('original','typed')),
 revision_id TEXT NOT NULL,
 PRIMARY KEY(note_id,representation),
 FOREIGN KEY(revision_id,note_id,representation) REFERENCES note_revisions(revision_id,note_id,representation) ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS note_published_by_revision ON note_published_representations(revision_id);
