UPDATE asset_descriptions SET asset_description = '' WHERE asset_description IS NULL;
ALTER TABLE asset_descriptions
ALTER COLUMN asset_description SET NOT NULL;

UPDATE asset_ext_tickers SET ext_ticker = '' WHERE ext_ticker IS NULL;
ALTER TABLE asset_ext_tickers
ALTER COLUMN ext_ticker SET NOT NULL;

UPDATE asset_names SET asset_name = '' WHERE asset_name IS NULL;
ALTER TABLE asset_names
ALTER COLUMN asset_name SET NOT NULL;

UPDATE asset_tickers SET ticker = '' WHERE ticker IS NULL;
ALTER TABLE asset_tickers
ALTER COLUMN ticker SET NOT NULL;
