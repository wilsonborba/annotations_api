-- Store the accepted 12-character Plane project identifier per owner container.
ALTER TABLE owner_plane_containers ADD COLUMN IF NOT EXISTS plane_identifier TEXT NOT NULL DEFAULT '';
