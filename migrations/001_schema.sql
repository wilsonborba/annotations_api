CREATE TABLE IF NOT EXISTS owners (id TEXT PRIMARY KEY, display_name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS languages (code TEXT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS topics (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS schema_versions (
 name TEXT NOT NULL, version BIGINT NOT NULL CHECK(version > 0),
 status TEXT NOT NULL CHECK(status IN ('draft','published','retired')),
 is_default BIGINT NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)),
 PRIMARY KEY(name,version)
);
CREATE TABLE IF NOT EXISTS schema_fields (
 schema_name TEXT NOT NULL, schema_version BIGINT NOT NULL,
 path TEXT NOT NULL, field_type TEXT NOT NULL, required BIGINT NOT NULL CHECK(required IN (0,1)),
 PRIMARY KEY(schema_name,schema_version,path),
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version)
);
CREATE TABLE IF NOT EXISTS posts (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL REFERENCES owners(id),
 slug TEXT NOT NULL UNIQUE, visibility TEXT NOT NULL CHECK(visibility IN ('public','private')),
 status TEXT NOT NULL CHECK(status IN ('draft','published')),
 cover_url TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS post_translations (
 post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
 language_code TEXT NOT NULL REFERENCES languages(code), title TEXT NOT NULL, summary TEXT NOT NULL,
 document_id TEXT NOT NULL UNIQUE, schema_name TEXT NOT NULL, schema_version BIGINT NOT NULL,
 PRIMARY KEY(post_id,language_code),
 FOREIGN KEY(schema_name,schema_version) REFERENCES schema_versions(name,version)
);
CREATE TABLE IF NOT EXISTS post_channels (
 post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
 channel_id TEXT NOT NULL REFERENCES channels(id), PRIMARY KEY(post_id,channel_id)
);
CREATE TABLE IF NOT EXISTS post_topics (
 post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
 topic_id TEXT NOT NULL REFERENCES topics(id), PRIMARY KEY(post_id,topic_id)
);
CREATE TABLE IF NOT EXISTS waitlist (
 email TEXT PRIMARY KEY, message TEXT NOT NULL, created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS public_posts ON posts(status,visibility,updated_at);
CREATE INDEX IF NOT EXISTS topic_posts ON post_topics(topic_id,post_id);
