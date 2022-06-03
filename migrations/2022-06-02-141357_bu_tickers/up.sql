CREATE TABLE IF NOT EXISTS asset_tickers (
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY,
    superseded_by BIGINT DEFAULT 9223372036854775806 NOT NULL,
    block_uid BIGINT NOT NULL CONSTRAINT data_entries_block_uid_fkey REFERENCES blocks_microblocks (uid) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    PRIMARY KEY (superseded_by, asset_id)
);


CREATE OR REPLACE FUNCTION reopen_asset_tickers() RETURNS VOID 
    language plpgsql 
AS $$
BEGIN
    UPDATE
        asset_tickers
    SET
        superseded_by = 9223372036854775806
    WHERE
    uid IN (
        SELECT
            al1.uid
        FROM
            asset_tickers al1
            LEFT JOIN asset_tickers al2 ON al1.superseded_by = al2.uid
        WHERE
            al1.superseded_by != 9223372036854775806
            AND al2.uid IS NULL
    );
END;
$$;


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
