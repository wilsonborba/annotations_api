-- v2 file cards: a note attachment may have any type. preview is 1 only when the bytes passed the
-- byte check of a preview type (images, PDF, CSV); every other file is a file card served as a download.
-- SQLite equivalent of migrations/018_v2_file_previews.sql. Only the column statement differs: the CHECK is part of it.
ALTER TABLE files ADD COLUMN preview BIGINT NOT NULL DEFAULT 0 CHECK(preview IN (0,1));
-- Ready files of a preview type passed the byte check of the earlier strict type list.
UPDATE files SET preview = 1 WHERE preview = 0 AND status = 'ready' AND content_type IN ('image/png','image/jpeg','image/gif','image/webp','image/avif','application/pdf','text/csv');
