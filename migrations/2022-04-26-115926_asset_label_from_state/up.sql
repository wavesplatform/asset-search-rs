ALTER TABLE asset_wx_labels ALTER COLUMN label TYPE TEXT;

UPDATE asset_wx_labels SET label = UPPER(label);

DROP TYPE IF EXISTS asset_wx_label_value_type;

CREATE TABLE IF NOT EXISTS asset_labels (
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY,
    superseded_by BIGINT DEFAULT 9223372036854775806 NOT NULL,
    block_uid BIGINT NOT NULL CONSTRAINT data_entries_block_uid_fkey REFERENCES blocks_microblocks (uid) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    labels TEXT[] NOT NULL,
    PRIMARY KEY (superseded_by, asset_id)
);
