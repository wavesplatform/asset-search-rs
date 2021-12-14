pub mod pg;

use anyhow::Result;

use super::models::asset::{
    AssetOverride, DeletedAsset, InsertableAsset, OracleDataEntry, QueryableAsset,
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

    fn mget_assets(&self, uids: &[i64]) -> Result<Vec<Option<QueryableAsset>>>;

    fn assets_oracle_data_entries(
        &self,
        asset_ids: &[&str],
        oracle_address: &str,
    ) -> Result<Vec<OracleDataEntry>>;

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
}
