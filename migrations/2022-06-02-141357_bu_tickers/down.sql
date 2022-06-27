CREATE TABLE IF NOT EXISTS predefined_verifications (
    asset_id TEXT NOT NULL CONSTRAINT predefined_verifications_pkey PRIMARY KEY,
    ticker TEXT DEFAULT NULL,
    verification_status verification_status_value_type DEFAULT 'unknown'
);

CREATE INDEX IF NOT EXISTS predefined_verifications_ticker_idx ON predefined_verifications (ticker);

CREATE INDEX IF NOT EXISTS predefined_verifications_verification_status_idx ON predefined_verifications (verification_status);

CREATE INDEX IF NOT EXISTS predefined_verifications_verification_status_asset_id_idx ON predefined_verifications (verification_status, asset_id);

DROP TABLE IF EXISTS asset_tickers;
DROP FUNCTION IF EXISTS reopen_asset_tickers;

CREATE OR REPLACE FUNCTION rollback_to(target_height INTEGER) RETURNS VOID 
    language plpgsql 
AS $$ 
BEGIN
    DELETE FROM blocks_microblocks WHERE height >= target_height;

    EXECUTE reopen_assets();
    EXECUTE reopen_asset_labels();
    EXECUTE reopen_data_entries();
    EXECUTE reopen_issuer_balances();
    EXECUTE reopen_out_leasings();
END;
$$;
