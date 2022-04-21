CREATE UNIQUE INDEX IF NOT EXISTS assets_uid_idx ON assets (uid);

CREATE INDEX IF NOT EXISTS assets_id_uid_desc_block_uid_partial_idx
    ON assets (id, uid DESC, block_uid) WHERE nft = false;
