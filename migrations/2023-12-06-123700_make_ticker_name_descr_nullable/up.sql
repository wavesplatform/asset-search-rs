ALTER TABLE asset_descriptions
ALTER COLUMN asset_description DROP NOT NULL;
UPDATE asset_descriptions SET asset_description = NULL WHERE asset_description = '';

ALTER TABLE asset_ext_tickers
ALTER COLUMN ext_ticker DROP NOT NULL;
UPDATE asset_ext_tickers SET ext_ticker = NULL WHERE ext_ticker = '';

ALTER TABLE asset_names
ALTER COLUMN asset_name DROP NOT NULL;
UPDATE asset_names SET asset_name = NULL WHERE asset_name = '';

ALTER TABLE asset_tickers
ALTER COLUMN ticker DROP NOT NULL;
UPDATE asset_tickers SET ticker = NULL WHERE ticker = '';


