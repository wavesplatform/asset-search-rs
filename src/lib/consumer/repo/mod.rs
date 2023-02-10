pub mod pg;

use anyhow::Result;

use crate::services::assets::repo::{Asset, OracleDataEntry, UserDefinedData};

use super::models::asset::{AssetOverride, DeletedAsset, InsertableAsset};
use super::models::asset_labels::{
    AssetLabels, AssetLabelsOverride, DeletedAssetLabels, InsertableAssetLabels,
};
use super::models::asset_tickers::{
    AssetTicker, AssetTickerOverride, DeletedAssetTicker, InsertableAssetTicker,
};
use super::models::block_microblock::BlockMicroblock;
use super::models::data_entry::{DataEntryOverride, DeletedDataEntry, InsertableDataEntry};
use super::models::issuer_balance::{
    CurrentIssuerBalance, DeletedIssuerBalance, InsertableIssuerBalance, IssuerBalanceOverride,
};
use super::models::out_leasing::{DeletedOutLeasing, InsertableOutLeasing, OutLeasingOverride};
use super::PrevHandledHeight;

#[async_trait::async_trait]
pub trait Repo {
    //
    // COMMON
    //

    fn transaction(&self, f: impl FnOnce() -> Result<()>) -> Result<()>;

    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&self) -> Result<i64>;

    fn get_total_block_id(&self) -> Result<Option<String>>;

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>>;

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn delete_microblocks(&self) -> Result<()>;

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()>;

    //
    // ASSETS
    //

    fn get_current_waves_quantity(&self) -> Result<i64>;

    fn get_next_assets_uid(&self) -> Result<i64>;

    fn insert_assets(&self, assets: &Vec<InsertableAsset>) -> Result<()>;

    fn update_assets_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_assets_superseded_by(&self, updates: &Vec<AssetOverride>) -> Result<()>;

    fn reopen_assets_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_assets_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_assets(&self, block_uid: &i64) -> Result<Vec<DeletedAsset>>;

    fn assets_gt_block_uid(&self, block_uid: &i64) -> Result<Vec<i64>>;

    fn mget_assets(&self, uids: &[i64]) -> Result<Vec<Option<Asset>>>;

    fn assets_oracle_data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>>;

    fn issuer_assets(&self, issuer_address: impl AsRef<str>) -> Result<Vec<Asset>>;

    //
    // ASSET LABELS
    //

    fn mget_asset_labels(&self, asset_ids: &[&str]) -> Result<Vec<AssetLabels>>;

    fn get_next_asset_labels_uid(&self) -> Result<i64>;

    fn insert_asset_labels(&self, balances: &Vec<InsertableAssetLabels>) -> Result<()>;

    fn update_asset_labels_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_asset_labels_superseded_by(&self, updates: &Vec<AssetLabelsOverride>) -> Result<()>;

    fn reopen_asset_labels_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_asset_labels_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_asset_labels(&self, block_uid: &i64) -> Result<Vec<DeletedAssetLabels>>;

    //
    // ASSET TICKERS
    //

    fn mget_asset_tickers(&self, asset_ids: &[&str]) -> Result<Vec<AssetTicker>>;

    fn get_next_asset_tickers_uid(&self) -> Result<i64>;

    fn insert_asset_tickers(&self, updates: &Vec<InsertableAssetTicker>) -> Result<()>;

    fn update_asset_tickers_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_asset_tickers_superseded_by(&self, updates: &Vec<AssetTickerOverride>) -> Result<()>;

    fn reopen_asset_tickers_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_asset_tickers_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_asset_tickers(&self, block_uid: &i64) -> Result<Vec<DeletedAssetTicker>>;

    //
    // DATA ENTRIES
    //

    fn get_next_data_entries_uid(&self) -> Result<i64>;

    fn insert_data_entries(&self, balances: &Vec<InsertableDataEntry>) -> Result<()>;

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_data_entries_superseded_by(&self, updates: &Vec<DataEntryOverride>) -> Result<()>;

    fn reopen_data_entries_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_data_entries_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>>;

    //
    // ISSUER BALANCES
    //

    fn get_current_issuer_balances(&self) -> Result<Vec<CurrentIssuerBalance>>;

    fn get_next_issuer_balances_uid(&self) -> Result<i64>;

    fn insert_issuer_balances(&self, balances: &Vec<InsertableIssuerBalance>) -> Result<()>;

    fn update_issuer_balances_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_issuer_balances_superseded_by(
        &self,
        updates: &Vec<IssuerBalanceOverride>,
    ) -> Result<()>;

    fn reopen_issuer_balances_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_issuer_balances_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_issuer_balances(&self, block_uid: &i64) -> Result<Vec<DeletedIssuerBalance>>;

    //
    // OUT LEASINGS
    //

    fn get_next_out_leasings_uid(&self) -> Result<i64>;

    fn insert_out_leasings(&self, balances: &Vec<InsertableOutLeasing>) -> Result<()>;

    fn update_out_leasings_block_references(&self, block_uid: &i64) -> Result<()>;

    fn close_out_leasings_superseded_by(&self, updates: &Vec<OutLeasingOverride>) -> Result<()>;

    fn reopen_out_leasings_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_out_leasings_next_update_uid(&self, new_uid: i64) -> Result<()>;

    fn rollback_out_leasings(&self, block_uid: &i64) -> Result<Vec<DeletedOutLeasing>>;

    //
    fn data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>>;

    fn mget_assets_by_ids(&self, ids: &[&str]) -> Result<Vec<Option<Asset>>>;

    fn mget_asset_user_defined_data(&self, asset_ids: &[&str]) -> Result<Vec<UserDefinedData>>;
}
