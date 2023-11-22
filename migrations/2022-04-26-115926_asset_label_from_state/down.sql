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


CREATE OR REPLACE FUNCTION rollback_to(target_height INTEGER) RETURNS VOID
    language plpgsql
AS $$
BEGIN
    DELETE FROM blocks_microblocks WHERE height >= target_height;

    EXECUTE reopen_assets();
    EXECUTE reopen_data_entries();
    EXECUTE reopen_issuer_balances();
    EXECUTE reopen_out_leasings();
END;
$$;

DROP FUNCTION reopen_asset_labels;
