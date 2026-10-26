CREATE TABLE IF NOT EXISTS media (
 id TEXT PRIMARY KEY, owner_id TEXT NOT NULL REFERENCES owners(id),
 filename TEXT NOT NULL, content_type TEXT NOT NULL, size_bytes BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS post_media (
 post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
 media_id TEXT NOT NULL REFERENCES media(id), PRIMARY KEY(post_id,media_id)
);
