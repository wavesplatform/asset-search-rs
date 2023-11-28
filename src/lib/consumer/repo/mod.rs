pub mod pg;

use anyhow::Result;

use crate::services::assets::repo::{Asset, OracleDataEntry, UserDefinedData};

use super::models::asset::{AssetOverride, DeletedAsset, InsertableAsset};
use super::models::asset_descriptions::{
    AssetDescriptionOverride, DeletedAssetDescription, InsertableAssetDescription,
};
use super::models::asset_labels::{
    AssetLabels, AssetLabelsOverride, DeletedAssetLabels, InsertableAssetLabels,
};
use super::models::asset_names::{AssetNameOverride, DeletedAssetName, InsertableAssetName};
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
    type Operations: RepoOperations;

    /// Execute some operations on a pooled connection without creating a database transaction.
    async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R> + Send + 'static,
        R: Send + 'static;

    /// Execute some operations within a database transaction.
    async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R> + Send + 'static,
        R: Clone + Send + 'static;
}

pub trait RepoOperations {
    //
    // COMMON
    //

    fn get_prev_handled_height(&mut self, depth: u32) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&mut self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&mut self) -> Result<i64>;

    fn get_total_block_id(&mut self) -> Result<Option<String>>;

    fn insert_blocks_or_microblocks(&mut self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>>;

    fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn delete_microblocks(&mut self) -> Result<()>;

    fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<()>;

    //
    // ASSETS
    //

    fn get_current_waves_quantity(&mut self) -> Result<i64>;

    fn get_next_assets_uid(&mut self) -> Result<i64>;

    fn insert_assets(&mut self, assets: &Vec<InsertableAsset>) -> Result<()>;

    fn update_assets_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_assets_superseded_by(&mut self, updates: &Vec<AssetOverride>) -> Result<()>;

    fn reopen_assets_superseded_by(&mut self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_assets_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_assets(&mut self, block_uid: &i64) -> Result<Vec<DeletedAsset>>;

    fn assets_gt_block_uid(&mut self, block_uid: &i64) -> Result<Vec<i64>>;

    fn mget_assets(&mut self, uids: &[i64]) -> Result<Vec<Option<Asset>>>;

    fn assets_oracle_data_entries(
        &mut self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>>;

    fn issuer_assets(&mut self, issuer_address: impl AsRef<str>) -> Result<Vec<Asset>>;

    //
    // ASSET LABELS
    //

    fn mget_asset_labels(&mut self, asset_ids: &[&str]) -> Result<Vec<AssetLabels>>;

    fn get_next_asset_labels_uid(&mut self) -> Result<i64>;

    fn insert_asset_labels(&mut self, balances: &Vec<InsertableAssetLabels>) -> Result<()>;

    fn update_asset_labels_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_asset_labels_superseded_by(
        &mut self,
        updates: &Vec<AssetLabelsOverride>,
    ) -> Result<()>;

    fn reopen_asset_labels_superseded_by(&mut self, current_superseded_by: &Vec<i64>)
        -> Result<()>;

    fn set_asset_labels_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_asset_labels(&mut self, block_uid: &i64) -> Result<Vec<DeletedAssetLabels>>;

    //
    // ASSET TICKERS
    //

    fn mget_asset_tickers(&mut self, asset_ids: &[&str]) -> Result<Vec<AssetTicker>>;

    fn get_next_asset_tickers_uid(&mut self) -> Result<i64>;

    fn insert_asset_tickers(&mut self, updates: &Vec<InsertableAssetTicker>) -> Result<()>;

    fn update_asset_tickers_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_asset_tickers_superseded_by(
        &mut self,
        updates: &Vec<AssetTickerOverride>,
    ) -> Result<()>;

    fn reopen_asset_tickers_superseded_by(
        &mut self,
        current_superseded_by: &Vec<i64>,
    ) -> Result<()>;

    fn set_asset_tickers_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_asset_tickers(&mut self, block_uid: &i64) -> Result<Vec<DeletedAssetTicker>>;

    //
    // DATA ENTRIES
    //

    fn get_next_data_entries_uid(&mut self) -> Result<i64>;

    fn insert_data_entries(&mut self, balances: &Vec<InsertableDataEntry>) -> Result<()>;

    fn update_data_entries_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_data_entries_superseded_by(&mut self, updates: &Vec<DataEntryOverride>) -> Result<()>;

    fn reopen_data_entries_superseded_by(&mut self, current_superseded_by: &Vec<i64>)
        -> Result<()>;

    fn set_data_entries_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>>;

    //
    // ISSUER BALANCES
    //

    fn get_current_issuer_balances(&mut self) -> Result<Vec<CurrentIssuerBalance>>;

    fn get_next_issuer_balances_uid(&mut self) -> Result<i64>;

    fn insert_issuer_balances(&mut self, balances: &Vec<InsertableIssuerBalance>) -> Result<()>;

    fn update_issuer_balances_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_issuer_balances_superseded_by(
        &mut self,
        updates: &Vec<IssuerBalanceOverride>,
    ) -> Result<()>;

    fn reopen_issuer_balances_superseded_by(
        &mut self,
        current_superseded_by: &Vec<i64>,
    ) -> Result<()>;

    fn set_issuer_balances_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_issuer_balances(&mut self, block_uid: &i64) -> Result<Vec<DeletedIssuerBalance>>;

    //
    // OUT LEASINGS
    //

    fn get_next_out_leasings_uid(&mut self) -> Result<i64>;

    fn insert_out_leasings(&mut self, balances: &Vec<InsertableOutLeasing>) -> Result<()>;

    fn update_out_leasings_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_out_leasings_superseded_by(&mut self, updates: &Vec<OutLeasingOverride>)
        -> Result<()>;

    fn reopen_out_leasings_superseded_by(&mut self, current_superseded_by: &Vec<i64>)
        -> Result<()>;

    fn set_out_leasings_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_out_leasings(&mut self, block_uid: &i64) -> Result<Vec<DeletedOutLeasing>>;

    //
    // ASSET_NAMES
    //

    fn get_next_asset_names_uid(&mut self) -> Result<i64>;

    fn insert_asset_names(&mut self, balances: &Vec<InsertableAssetName>) -> Result<()>;

    fn update_asset_names_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_asset_names_superseded_by(&mut self, updates: &Vec<AssetNameOverride>) -> Result<()>;

    fn reopen_asset_names_superseded_by(&mut self, current_superseded_by: &Vec<i64>) -> Result<()>;

    fn set_asset_names_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_asset_names(&mut self, block_uid: &i64) -> Result<Vec<DeletedAssetName>>;

    //
    // ASSET_DESCRIPTIONS
    //

    fn get_next_asset_descriptions_uid(&mut self) -> Result<i64>;

    fn insert_asset_descriptions(
        &mut self,
        balances: &Vec<InsertableAssetDescription>,
    ) -> Result<()>;

    fn update_asset_descriptions_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_asset_descriptions_superseded_by(
        &mut self,
        updates: &Vec<AssetDescriptionOverride>,
    ) -> Result<()>;

    fn reopen_asset_descriptions_superseded_by(
        &mut self,
        current_superseded_by: &Vec<i64>,
    ) -> Result<()>;

    fn set_asset_descriptions_next_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_asset_descriptions(
        &mut self,
        block_uid: &i64,
    ) -> Result<Vec<DeletedAssetDescription>>;

    // Methods needed for updates redis cache
    fn data_entries(
        &mut self,
        asset_ids: &[String],
        oracle_address: String,
    ) -> Result<Vec<OracleDataEntry>>;

    fn mget_assets_by_ids(&mut self, ids: &[String]) -> Result<Vec<Option<Asset>>>;

    fn mget_asset_user_defined_data(
        &mut self,
        asset_ids: &[String],
    ) -> Result<Vec<UserDefinedData>>;

    fn get_last_asset_ids_by_issuers(&mut self, issuers_ids: &[String]) -> Result<Vec<String>>;
}
