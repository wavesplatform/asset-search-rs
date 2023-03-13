DROP TABLE IF EXISTS asset_names;
DROP TABLE IF EXISTS asset_descriptions;


DROP FUNCTION IF EXISTS reopen_asset_names();
DROP FUNCTION IF EXISTS reopen_asset_descriptions();

CREATE OR REPLACE FUNCTION rollback_to(target_height INTEGER) RETURNS VOID
    language plpgsql
AS $$
BEGIN
    DELETE FROM blocks_microblocks WHERE height >= target_height;

    EXECUTE reopen_assets();
    EXECUTE reopen_asset_labels();
    EXECUTE reopen_asset_tickers();
    EXECUTE reopen_data_entries();
    EXECUTE reopen_issuer_balances();
    EXECUTE reopen_out_leasings();
END;
$$;
