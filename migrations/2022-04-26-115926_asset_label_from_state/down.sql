DROP TABLE IF EXISTS asset_labels;

CREATE TYPE asset_wx_label_value_type AS ENUM (
    'defi',
    'gateway',
    'stablecoin',
    'qualified',
    'wa_verified',
    'community_verified'
);

ALTER TABLE asset_wx_labels ALTER COLUMN label TYPE asset_wx_label_value_type USING LOWER(label)::asset_wx_label_value_type;
