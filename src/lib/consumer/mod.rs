pub mod models;
pub mod repo;
pub mod updates;

use anyhow::{Error, Result};
use bigdecimal::ToPrimitive;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::Receiver;
use waves_protobuf_schemas::waves::events::state_update::{BalanceUpdate, LeasingUpdate};
use waves_protobuf_schemas::waves::{
    data_transaction_data::data_entry::Value,
    events::{StateUpdate, TransactionMetadata},
    SignedTransaction,
};
use wavesexchange_log::{debug, info, timer};

use self::models::asset::{Asset, AssetOverride, DeletedAsset, InsertableAsset};
use self::models::block_microblock::BlockMicroblock;
use self::models::data_entry::{
    DataEntryOverride, DataEntryUpdate, DataEntryValue, DeletedDataEntry, InsertableDataEntry,
};
use self::models::issuer_balance::{
    DeletedIssuerBalance, InsertableIssuerBalance, IssuerBalanceOverride, IssuerBalanceUpdate,
};
use self::models::out_leasing::{
    DeletedOutLeasing, InsertableOutLeasing, OutLeasingOverride, OutLeasingUpdate,
};
use crate::cache::{AssetBlockchainData, AssetUserDefinedData, SyncReadCache, SyncWriteCache};
use crate::db::enums::DataEntryValueType;
use crate::error::Error as AppError;
use crate::models::{AssetInfoUpdate, AssetLabel, AssetOracleDataEntry, DataEntryType};
use crate::waves::{get_asset_id, is_waves_asset_id, Address, WAVES_ID};

lazy_static! {
    static ref ASSET_ORACLE_DATA_ENTRY_KEY_REGEX: Regex =
        Regex::new(r"^.+_<([a-zA-Z\d]+)>$").unwrap();
}

const ASSET_ORACLE_VERIFICATION_STATUS_VERIFIED: i64 = 2;

#[derive(Clone, Debug)]
pub enum BlockchainUpdate {
    Block(BlockMicroblockAppend),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Clone, Debug)]
pub struct BlockMicroblockAppend {
    id: String,
    time_stamp: Option<i64>,
    height: u32,
    updated_waves_amount: Option<i64>,
    state_update: StateUpdate,
    txs: Vec<Tx>,
}

#[derive(Clone, Debug)]
pub struct Tx {
    pub id: String,
    pub data: SignedTransaction,
    pub meta: TransactionMetadata,
    pub state_update: StateUpdate,
}

#[derive(Debug)]
pub struct BlockchainUpdatesWithLastHeight {
    pub last_height: u32,
    pub updates: Vec<BlockchainUpdate>,
}

#[derive(Debug, Queryable)]
pub struct PrevHandledHeight {
    pub uid: i64,
    pub height: i32,
}

#[derive(Debug)]
enum UpdatesItem {
    Blocks(Vec<BlockMicroblockAppend>),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Debug)]
enum AssetLabelUpdate {
    SetLabel(AssetLabel),
    DeleteLabel(AssetLabel),
}

#[async_trait::async_trait]
pub trait UpdatesSource {
    async fn stream(
        self,
        from_height: u32,
        batch_max_size: usize,
        batch_max_time: Duration,
    ) -> Result<Receiver<BlockchainUpdatesWithLastHeight>, AppError>;
}

// TODO: handle shutdown signals -> rollback current transaction
pub async fn start<T, R, CBD, CUDD>(
    starting_height: u32,
    updates_src: T,
    repo: Arc<R>,
    blockchain_data_cache: CBD,
    user_defined_data_cache: CUDD,
    updates_per_request: usize,
    max_wait_time_in_secs: u64,
    chain_id: u8,
    waves_association_address: &str,
) -> Result<()>
where
    T: UpdatesSource + Send + Sync + 'static,
    R: repo::Repo,
    CBD: SyncReadCache<AssetBlockchainData> + SyncWriteCache<AssetBlockchainData> + Clone,
    CUDD: SyncReadCache<AssetUserDefinedData> + SyncWriteCache<AssetUserDefinedData> + Clone,
{
    let starting_from_height = match repo.get_prev_handled_height()? {
        Some(prev_handled_height) => {
            repo.transaction(|| {
                rollback(
                    repo.clone(),
                    blockchain_data_cache.clone(),
                    user_defined_data_cache.clone(),
                    waves_association_address,
                    prev_handled_height.uid,
                )
            })?;
            prev_handled_height.height as u32 + 1
        }
        None => starting_height,
    };

    info!(
        "Start fetching updates from height {}",
        starting_from_height
    );
    let max_duration = Duration::seconds(max_wait_time_in_secs.to_i64().unwrap());

    let mut rx = updates_src
        .stream(starting_from_height, updates_per_request, max_duration)
        .await?;

    loop {
        let mut start = Instant::now();

        let updates_with_height = rx.recv().await.ok_or(Error::new(AppError::StreamClosed(
            "GRPC Stream was closed by the server".to_string(),
        )))?;

        let updates_count = updates_with_height.updates.len();
        info!(
            "{} updates were received in {:?}",
            updates_count,
            start.elapsed()
        );

        let last_height = updates_with_height.last_height;

        start = Instant::now();

        repo.transaction(|| {
            handle_updates(
                updates_with_height,
                repo.clone(),
                blockchain_data_cache.clone(),
                user_defined_data_cache.clone(),
                chain_id,
                waves_association_address,
            )?;

            info!(
                "{} updates were handled in {:?} ms. Last updated height is {}.",
                updates_count,
                start.elapsed().as_millis(),
                last_height
            );

            Ok(())
        })?;
    }
}

fn handle_updates<'a, R, CBD, CUDD>(
    updates_with_height: BlockchainUpdatesWithLastHeight,
    repo: Arc<R>,
    blockchain_data_cache: CBD,
    user_defined_data_cache: CUDD,
    chain_id: u8,
    waves_association_address: &str,
) -> Result<()>
where
    R: repo::Repo,
    CBD: SyncReadCache<AssetBlockchainData> + SyncWriteCache<AssetBlockchainData> + Clone,
    CUDD: SyncReadCache<AssetUserDefinedData> + SyncWriteCache<AssetUserDefinedData> + Clone,
{
    updates_with_height
        .updates
        .into_iter()
        .fold::<&mut Vec<UpdatesItem>, _>(&mut vec![], |acc, cur| match cur {
            BlockchainUpdate::Block(b) => {
                info!("Handle block {}, height = {}", b.id, b.height);
                let len = acc.len();
                if acc.len() > 0 {
                    match acc.iter_mut().nth(len as usize - 1).unwrap() {
                        UpdatesItem::Blocks(v) => {
                            v.push(b);
                            acc
                        }
                        UpdatesItem::Microblock(_) | UpdatesItem::Rollback(_) => {
                            acc.push(UpdatesItem::Blocks(vec![b]));
                            acc
                        }
                    }
                } else {
                    acc.push(UpdatesItem::Blocks(vec![b]));
                    acc
                }
            }
            BlockchainUpdate::Microblock(mba) => {
                info!("Handle microblock {}, height = {}", mba.id, mba.height);
                acc.push(UpdatesItem::Microblock(mba));
                acc
            }
            BlockchainUpdate::Rollback(sig) => {
                info!("Handle rollback to {}", sig);
                acc.push(UpdatesItem::Rollback(sig));
                acc
            }
        })
        .into_iter()
        .try_fold((), |_, update_item| match update_item {
            UpdatesItem::Blocks(bs) => {
                squash_microblocks(repo.clone())?;
                handle_appends(
                    repo.clone(),
                    blockchain_data_cache.clone(),
                    user_defined_data_cache.clone(),
                    chain_id,
                    bs.as_ref(),
                    waves_association_address,
                )
            }
            UpdatesItem::Microblock(mba) => handle_appends(
                repo.clone(),
                blockchain_data_cache.clone(),
                user_defined_data_cache.clone(),
                chain_id,
                &vec![mba.to_owned()],
                waves_association_address,
            ),
            UpdatesItem::Rollback(sig) => {
                let block_uid = repo.clone().get_block_uid(&sig)?;
                rollback(
                    repo.clone(),
                    blockchain_data_cache.clone(),
                    user_defined_data_cache.clone(),
                    waves_association_address,
                    block_uid,
                )
            }
        })?;

    Ok(())
}

fn handle_appends<'a, R, CBD, CUDD>(
    repo: Arc<R>,
    blockchain_data_cache: CBD,
    user_defined_data_cache: CUDD,
    chain_id: u8,
    appends: &Vec<BlockMicroblockAppend>,
    waves_association_address: &str,
) -> Result<()>
where
    R: repo::Repo,
    CBD: SyncReadCache<AssetBlockchainData> + SyncWriteCache<AssetBlockchainData> + Clone,
    CUDD: SyncReadCache<AssetUserDefinedData> + SyncWriteCache<AssetUserDefinedData> + Clone,
{
    let block_uids = repo.insert_blocks_or_microblocks(
        &appends
            .into_iter()
            .map(|append| BlockMicroblock {
                id: append.id.clone(),
                height: append.height as i32,
                time_stamp: append.time_stamp,
            })
            .collect_vec(),
    )?;

    let block_uids_with_appends = block_uids.into_iter().zip(appends).collect_vec();

    let assets_updates_with_block_uids = {
        timer!("assets updates handling");

        let assets_updates_with_block_uids: Vec<(&i64, Asset)> = block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                extract_asset_updates(chain_id, append)
                    .into_iter()
                    .map(|au| (block_uid, au))
                    .collect_vec()
            })
            .collect();

        handle_assets_updates(repo.clone(), &assets_updates_with_block_uids)?;

        info!(
            "handled {} assets updates",
            assets_updates_with_block_uids.len()
        );

        assets_updates_with_block_uids
    };

    let data_entries_updates_with_block_uids = {
        timer!("data entries updates handling");

        let data_entries_updates_with_block_uids: Vec<(&i64, DataEntryUpdate)> =
            block_uids_with_appends
                .iter()
                .flat_map(|(block_uid, append)| {
                    append
                        .txs
                        .iter()
                        .flat_map(|tx| {
                            extract_data_entries_updates(
                                append.height as i32,
                                tx,
                                waves_association_address,
                            )
                        })
                        .map(|u| (block_uid, u))
                        .collect_vec()
                })
                .collect();

        handle_data_entries_updates(repo.clone(), &data_entries_updates_with_block_uids)?;

        info!(
            "handled {} data entries updates",
            data_entries_updates_with_block_uids.len()
        );

        data_entries_updates_with_block_uids
    };

    let issuer_balances_updates_with_block_uids = {
        timer!("issuer balances updates handling");

        let current_issuer_balances = repo.get_current_issuer_balances()?;

        let issuers = assets_updates_with_block_uids
            .iter()
            .filter(|(_, au)| au.id != WAVES_ID)
            .map(|(_, au)| au.issuer.as_ref())
            .chain(
                current_issuer_balances
                    .iter()
                    .map(|cib| cib.address.as_ref()),
            )
            .fold(HashSet::new(), |mut acc, cur| {
                acc.insert(cur);
                acc
            });

        let issuer_balances_updates_with_block_uids: Vec<(&i64, IssuerBalanceUpdate)> =
            block_uids_with_appends
                .iter()
                .flat_map(|(block_uid, append)| {
                    let balance_updates = extract_all_balance_updates(append);
                    extract_issuer_balances_updates(&balance_updates, &issuers)
                        .into_iter()
                        .map(|u| (block_uid, u))
                        .collect_vec()
                })
                .collect();

        handle_issuer_balances_updates(repo.clone(), &issuer_balances_updates_with_block_uids)?;

        info!(
            "handled {} issuer balances updates",
            issuer_balances_updates_with_block_uids.len()
        );

        issuer_balances_updates_with_block_uids
    };

    let out_leasing_updates_with_block_uids = {
        timer!("out leasing updates handling");

        let out_leasing_updates_with_block_uids: Vec<(&i64, OutLeasingUpdate)> =
            block_uids_with_appends
                .iter()
                .flat_map(|(block_uid, append)| {
                    let updates = extract_all_leasing_updates(append);
                    extract_out_leasing_updates(&updates)
                        .into_iter()
                        .map(|u| (block_uid, u))
                        .collect_vec()
                })
                .collect();

        handle_out_leasing_updates(repo.clone(), &out_leasing_updates_with_block_uids)?;

        info!(
            "handled {} out leasing updates",
            out_leasing_updates_with_block_uids.len()
        );

        out_leasing_updates_with_block_uids
    };

    let mut assets_info_updates = assets_updates_with_block_uids.iter().fold(
        HashMap::new(),
        |mut acc: HashMap<String, (Asset, AssetInfoUpdate)>, (_block_uid, asset)| {
            acc.entry(asset.id.clone())
                .and_modify(|(asset, asset_info_update)| {
                    asset_info_update.name = Some(asset.name.clone());
                    asset_info_update.description = Some(asset.description.clone());
                    asset_info_update.quantity = Some(asset.quantity);
                    asset_info_update.reissuable = Some(asset.reissuable);
                    asset_info_update.smart = Some(asset.smart);
                    asset_info_update.min_sponsored_fee = asset.min_sponsored_fee;
                })
                .or_insert((
                    asset.to_owned(),
                    AssetInfoUpdate {
                        id: asset.id.clone(),
                        updated_at: asset.time_stamp,
                        update_height: asset.height,
                        name: Some(asset.name.clone()),
                        description: Some(asset.description.clone()),
                        quantity: Some(asset.quantity),
                        reissuable: Some(asset.reissuable),
                        smart: Some(asset.smart),
                        min_sponsored_fee: asset.min_sponsored_fee,
                        sponsor_regular_balance: None,
                        sponsor_out_leasing: None,
                        oracles_data: None,
                    },
                ));
            acc
        },
    );

    data_entries_updates_with_block_uids
        .iter()
        .filter_map(|(_, de_update)| {
            de_update
                .related_asset_id
                .as_ref()
                .map(|related_asset_id| (related_asset_id, de_update))
        })
        .for_each(|(related_asset_id, de_update)| {
            assets_info_updates
                .entry(related_asset_id.to_owned())
                .and_modify(|(_asset, current_asset_info_update)| {
                    if let Some(asset_oracles_data) =
                        current_asset_info_update.oracles_data.as_mut()
                    {
                        let asset_oracle_data = asset_oracles_data
                            .entry(de_update.address.clone())
                            .or_default();

                        let asset_oracle_data_entry: Option<AssetOracleDataEntry> =
                            de_update.into();

                        if let Some(asset_oracle_data_entry) = asset_oracle_data_entry {
                            asset_oracle_data.push(asset_oracle_data_entry);
                        }
                    } else {
                        let mut asset_oracles_data = HashMap::new();
                        let mut asset_oracle_data = vec![];

                        let asset_oracle_data_entry: Option<AssetOracleDataEntry> =
                            de_update.into();

                        if let Some(asset_oracle_data_entry) = asset_oracle_data_entry {
                            asset_oracle_data.push(asset_oracle_data_entry);
                        }

                        asset_oracles_data.insert(de_update.address.clone(), asset_oracle_data);

                        current_asset_info_update.oracles_data = Some(asset_oracles_data);
                    }
                });
        });

    issuer_balances_updates_with_block_uids
        .iter()
        .for_each(|(_, ib_update)| {
            assets_info_updates
                .iter_mut()
                .filter(|(_, (asset, _asset_info_update))| asset.issuer == ib_update.address)
                .for_each(|(_, (_asset, asset_info_update))| {
                    asset_info_update.sponsor_regular_balance = Some(ib_update.new_regular_balance);
                });
        });

    out_leasing_updates_with_block_uids
        .iter()
        .for_each(|(_, ol_update)| {
            assets_info_updates
                .iter_mut()
                .filter(|(_, (asset, _asset_info_update))| asset.issuer == ol_update.address)
                .for_each(|(_, (_asset, asset_info_update))| {
                    asset_info_update.sponsor_out_leasing = Some(ol_update.new_amount);
                });
        });

    let assets_info_updates_ids = assets_info_updates
        .keys()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    let cached_blochain_data = blockchain_data_cache
        .mget(&assets_info_updates_ids)?
        .into_iter()
        .zip(&assets_info_updates_ids)
        .fold(
            HashMap::with_capacity(assets_info_updates_ids.len()),
            |mut acc, (o, asset_id)| {
                acc.insert(asset_id.to_owned(), o);
                acc
            },
        );

    let cached_user_defined_data = user_defined_data_cache
        .mget(&assets_info_updates_ids)?
        .into_iter()
        .zip(&assets_info_updates_ids)
        .fold(
            HashMap::with_capacity(assets_info_updates_ids.len()),
            |mut acc, (o, asset_id)| {
                acc.insert(asset_id.to_owned(), o);
                acc
            },
        );

    assets_info_updates
        .iter()
        .try_for_each::<_, Result<(), AppError>>(|(asset_id, (asset, asset_update))| {
            match cached_blochain_data
                .get(asset_id.as_str())
                .and_then(|o| o.as_ref())
            {
                Some(cached) => {
                    let new_asset_blockchain_data =
                        AssetBlockchainData::from((cached, asset_update));
                    blockchain_data_cache.set(&asset_id, new_asset_blockchain_data)?;
                }
                _ => {
                    let new_asset_blockchain_data =
                        AssetBlockchainData::from((asset, asset_update));
                    blockchain_data_cache.set(&asset_id, new_asset_blockchain_data)?;
                }
            }

            let wa_verified_asset_label_update = asset_update
                .oracles_data
                .as_ref()
                .and_then(|oracles_data| extract_wa_verified_asset_label_update(oracles_data));

            if let Some(wa_verified_asset_label_update) = wa_verified_asset_label_update {
                let current_asset_user_defined_data = match cached_user_defined_data
                    .get(asset_id.as_str())
                    .and_then(|o| o.clone())
                {
                    Some(cached) => cached,
                    _ => AssetUserDefinedData {
                        asset_id: asset_id.clone(),
                        ticker: None,
                        verification_status: crate::models::VerificationStatus::Unknown,
                        labels: vec![],
                    },
                };

                let new_asset_user_defined_data = match wa_verified_asset_label_update {
                    AssetLabelUpdate::SetLabel(label) => {
                        current_asset_user_defined_data.add_label(&label)
                    }
                    AssetLabelUpdate::DeleteLabel(label) => {
                        current_asset_user_defined_data.delete_label(&label)
                    }
                };

                user_defined_data_cache.set(&asset_id, new_asset_user_defined_data)?;
            }

            Ok(())
        })?;

    Ok(())
}

fn extract_asset_updates(chain_id: u8, append: &BlockMicroblockAppend) -> Vec<Asset> {
    let mut asset_updates = vec![];

    let update_time_stamp = match append.time_stamp {
        Some(time_stamp) => {
            DateTime::from_utc(NaiveDateTime::from_timestamp(time_stamp / 1000, 0), Utc)
        }
        None => Utc::now(),
    };

    if let Some(updated_waves_amount) = append.updated_waves_amount {
        asset_updates.push(Asset::waves_update(
            append.height as i32,
            update_time_stamp,
            updated_waves_amount,
        ));
    }

    let mut updates_from_txs = append
        .txs
        .iter()
        .flat_map(|tx| {
            tx.state_update
                .assets
                .iter()
                .filter_map(|asset_update| {
                    if let Some(asset_details) = &asset_update.after {
                        let time_stamp = DateTime::from_utc(
                            NaiveDateTime::from_timestamp(
                                tx.data
                                    .transaction
                                    .as_ref()
                                    .map(|t| t.timestamp / 1000)
                                    .unwrap(),
                                0,
                            ),
                            Utc,
                        );
                        let asset_id = get_asset_id(&asset_details.asset_id);
                        let issuer =
                            Address::from((asset_details.issuer.as_slice(), chain_id)).into();
                        Some(Asset {
                            height: append.height as i32,
                            time_stamp,
                            id: asset_id,
                            name: escape_unicode_null(&asset_details.name),
                            description: escape_unicode_null(&asset_details.description),
                            issuer: issuer,
                            precision: asset_details.decimals,
                            smart: asset_details
                                .script_info
                                .as_ref()
                                .map(|s| !s.script.is_empty() && true)
                                .unwrap_or(false),
                            nft: asset_details.nft,
                            reissuable: asset_details.reissuable,
                            min_sponsored_fee: if asset_details.sponsorship > 0 {
                                Some(asset_details.sponsorship)
                            } else {
                                None
                            },
                            quantity: asset_details.volume.to_owned(),
                        })
                    } else {
                        None
                    }
                })
                .collect_vec()
        })
        .collect_vec();

    asset_updates.append(&mut updates_from_txs);
    asset_updates
}

fn handle_assets_updates<R: repo::Repo>(repo: Arc<R>, updates: &[(&i64, Asset)]) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let assets_next_uid = repo.get_next_assets_uid()?;

    let current_waves_quantity = repo.get_current_waves_quantity()?;

    let asset_updates = updates
        .iter()
        .filter(|(_, update)| {
            // save only not-waves assets or waves quantity updates
            if update.id != WAVES_ID || update.quantity != current_waves_quantity {
                true
            } else {
                false
            }
        })
        .enumerate()
        .map(|(update_idx, (block_uid, update))| InsertableAsset {
            uid: assets_next_uid + update_idx as i64,
            superseded_by: -1,
            block_uid: *block_uid.clone(),
            id: update.id.clone(),
            name: update.name.clone(),
            description: update.description.clone(),
            time_stamp: update.time_stamp,
            issuer: update.issuer.clone(),
            precision: update.precision,
            smart: update.smart,
            nft: update.nft,
            quantity: update.quantity,
            reissuable: update.reissuable,
            min_sponsored_fee: update.min_sponsored_fee,
        })
        .collect_vec();

    let mut assets_grouped: HashMap<InsertableAsset, Vec<InsertableAsset>> = HashMap::new();

    asset_updates.into_iter().for_each(|update| {
        let group = assets_grouped.entry(update.clone()).or_insert(vec![]);
        group.push(update);
    });

    let assets_grouped = assets_grouped.into_iter().collect_vec();

    let assets_grouped_with_uids_superseded_by = assets_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableAsset>>();

            let mut last_uid = std::i64::MAX - 1;
            (
                group_key,
                updates
                    .as_mut_slice()
                    .iter_mut()
                    .rev()
                    .map(|cur| {
                        cur.superseded_by = last_uid;
                        last_uid = cur.uid;
                        cur.to_owned()
                    })
                    .sorted_by_key(|item| item.uid)
                    .collect(),
            )
        })
        .collect::<Vec<(InsertableAsset, Vec<InsertableAsset>)>>();

    // First uid for each asset in a new batch. This value closes superseded_by of previous updates.
    let assets_first_uids: Vec<AssetOverride> = assets_grouped_with_uids_superseded_by
        .iter()
        .map(|(_, group)| {
            let first = group.iter().next().unwrap().clone();
            AssetOverride {
                superseded_by: first.uid,
                id: first.id,
            }
        })
        .collect();

    repo.close_assets_superseded_by(&assets_first_uids)?;

    let assets_with_uids_superseded_by = &assets_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|asset| asset.uid)
        .collect_vec();

    repo.insert_assets(assets_with_uids_superseded_by)?;

    repo.set_assets_next_update_uid(assets_next_uid + updates_count as i64)
}

fn extract_data_entries_updates(
    height: i32,
    tx: &Tx,
    waves_association_address: &str,
) -> Vec<DataEntryUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if waves_association_address == &oracle_address {
                    let related_asset_id = parse_related_asset_id(&de.key);
                    Some(DataEntryUpdate {
                        height,
                        address: oracle_address,
                        key: de.key.clone(),
                        value: de.value.as_ref().map(|v| match v {
                            Value::BinaryValue(value) => DataEntryValue::BinVal(value.to_owned()),
                            Value::BoolValue(value) => DataEntryValue::BoolVal(value.to_owned()),
                            Value::IntValue(value) => DataEntryValue::IntVal(value.to_owned()),
                            Value::StringValue(value) => {
                                DataEntryValue::StrVal(escape_unicode_null(value))
                            }
                        }),
                        related_asset_id,
                    })
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

fn handle_data_entries_updates<R: repo::Repo>(
    repo: Arc<R>,
    updates: &[(&i64, DataEntryUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let data_entries_next_uid = repo.get_next_data_entries_uid()?;

    let data_entries_updates = updates
        .iter()
        .enumerate()
        .map(|(update_idx, (block_uid, update))| {
            let (data_type, bin_val, bool_val, int_val, str_val) = match &update.value {
                Some(DataEntryValue::BinVal(v)) => (
                    Some(DataEntryValueType::Bin),
                    Some(v.to_owned()),
                    None,
                    None,
                    None,
                ),
                Some(DataEntryValue::BoolVal(v)) => (
                    Some(DataEntryValueType::Bool),
                    None,
                    Some(v.to_owned()),
                    None,
                    None,
                ),
                Some(DataEntryValue::IntVal(v)) => (
                    Some(DataEntryValueType::Int),
                    None,
                    None,
                    Some(v.to_owned()),
                    None,
                ),
                Some(DataEntryValue::StrVal(v)) => (
                    Some(DataEntryValueType::Str),
                    None,
                    None,
                    None,
                    Some(v.to_owned()),
                ),
                None => (None, None, None, None, None),
            };
            InsertableDataEntry {
                uid: data_entries_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                address: update.address.clone(),
                key: update.key.clone(),
                data_type,
                bool_val,
                bin_val,
                int_val,
                str_val,
                related_asset_id: update.related_asset_id.clone(),
            }
        })
        .collect_vec();

    let mut data_entries_grouped: HashMap<InsertableDataEntry, Vec<InsertableDataEntry>> =
        HashMap::new();

    data_entries_updates.into_iter().for_each(|update| {
        let group = data_entries_grouped.entry(update.clone()).or_insert(vec![]);
        group.push(update);
    });

    let data_entries_grouped = data_entries_grouped.into_iter().collect_vec();

    let data_entries_grouped_with_uids_superseded_by = data_entries_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableDataEntry>>();

            let mut last_uid = std::i64::MAX - 1;
            (
                group_key,
                updates
                    .as_mut_slice()
                    .iter_mut()
                    .rev()
                    .map(|cur| {
                        cur.superseded_by = last_uid;
                        last_uid = cur.uid;
                        cur.to_owned()
                    })
                    .sorted_by_key(|item| item.uid)
                    .collect(),
            )
        })
        .collect::<Vec<(InsertableDataEntry, Vec<InsertableDataEntry>)>>();

    let data_entries_first_uids: Vec<DataEntryOverride> =
        data_entries_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                DataEntryOverride {
                    superseded_by: first.uid,
                    address: first.address,
                    key: first.key,
                }
            })
            .collect();

    repo.close_data_entries_superseded_by(&data_entries_first_uids)?;

    let data_entries_with_uids_superseded_by = &data_entries_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|data_entry| data_entry.uid)
        .collect_vec();

    repo.insert_data_entries(data_entries_with_uids_superseded_by)?;

    repo.set_data_entries_next_update_uid(data_entries_next_uid + updates_count as i64)
}

fn extract_all_balance_updates(append: &BlockMicroblockAppend) -> Vec<BalanceUpdate> {
    // at first, balance updates placed at append.state_update
    // at second, balance updates placed at append.txs[i].state_update
    // so balance updates from txs[i].state_update should override balance updates from append.state_update

    append
        .state_update
        .balances
        .clone()
        .into_iter()
        .chain(
            append
                .txs
                .iter()
                .flat_map(|tx| tx.state_update.balances.clone()),
        )
        .collect()
}

fn extract_issuer_balances_updates(
    balance_updates: &Vec<BalanceUpdate>,
    issuers: &HashSet<&str>,
) -> Vec<IssuerBalanceUpdate> {
    let mut issuer_balance_updates = HashMap::new();

    balance_updates
        .iter()
        .filter_map(|b| {
            let address = bs58::encode(&b.address).into_string();
            if issuers.contains(&address.as_str()) {
                b.amount_after.as_ref().and_then(|a| {
                    if is_waves_asset_id(&a.asset_id) && b.amount_before != a.amount {
                        Some((address, a.amount))
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        })
        .for_each(|(address, amount_after)| {
            issuer_balance_updates.insert(
                address.clone(),
                IssuerBalanceUpdate {
                    address,
                    new_regular_balance: amount_after,
                },
            );
        });

    issuer_balance_updates.into_values().collect_vec()
}

fn handle_issuer_balances_updates<R: repo::Repo>(
    repo: Arc<R>,
    updates: &[(&i64, IssuerBalanceUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let issuer_balances_next_uid = repo.get_next_issuer_balances_uid()?;

    let issuer_balances_updates = updates
        .iter()
        .enumerate()
        .map(
            |(update_idx, (block_uid, update))| InsertableIssuerBalance {
                uid: issuer_balances_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                address: update.address.clone(),
                regular_balance: update.new_regular_balance,
            },
        )
        .collect_vec();

    let mut issuer_balances_grouped: HashMap<
        InsertableIssuerBalance,
        Vec<InsertableIssuerBalance>,
    > = HashMap::new();

    issuer_balances_updates.into_iter().for_each(|update| {
        let group = issuer_balances_grouped
            .entry(update.clone())
            .or_insert(vec![]);
        group.push(update);
    });

    let issuer_balances_grouped = issuer_balances_grouped.into_iter().collect_vec();

    let issuer_balances_grouped_with_uids_superseded_by = issuer_balances_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableIssuerBalance>>();

            let mut last_uid = std::i64::MAX - 1;
            (
                group_key,
                updates
                    .as_mut_slice()
                    .iter_mut()
                    .rev()
                    .map(|cur| {
                        cur.superseded_by = last_uid;
                        last_uid = cur.uid;
                        cur.to_owned()
                    })
                    .sorted_by_key(|item| item.uid)
                    .collect(),
            )
        })
        .collect::<Vec<(InsertableIssuerBalance, Vec<InsertableIssuerBalance>)>>();

    let issuer_balances_first_uids: Vec<IssuerBalanceOverride> =
        issuer_balances_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                IssuerBalanceOverride {
                    superseded_by: first.uid,
                    address: first.address,
                }
            })
            .collect();

    repo.close_issuer_balances_superseded_by(&issuer_balances_first_uids)?;

    let issuer_balances_with_uids_superseded_by = &issuer_balances_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|issuer_balance| issuer_balance.uid)
        .collect_vec();

    repo.insert_issuer_balances(issuer_balances_with_uids_superseded_by)?;

    repo.set_issuer_balances_next_update_uid(issuer_balances_next_uid + updates_count as i64)
}

fn extract_all_leasing_updates(append: &BlockMicroblockAppend) -> Vec<LeasingUpdate> {
    // at first, balance updates placed at append.state_update
    // at second, balance updates placed at append.txs[i].state_update
    // so balance updates from txs[i].state_update should override balance updates from append.state_update

    append
        .state_update
        .leasing_for_address
        .clone()
        .into_iter()
        .chain(
            append
                .txs
                .iter()
                .flat_map(|tx| tx.state_update.leasing_for_address.clone()),
        )
        .collect::<Vec<_>>()
}

fn extract_out_leasing_updates(updates: &Vec<LeasingUpdate>) -> Vec<OutLeasingUpdate> {
    let mut out_leasing_updates = HashMap::new();

    updates.iter().filter(|lu| lu.out_after > 0).for_each(|lu| {
        let address = bs58::encode(&lu.address).into_string();
        if out_leasing_updates.contains_key(&address) {
            out_leasing_updates
                .entry(address.clone())
                .and_modify(|u: &mut OutLeasingUpdate| {
                    u.new_amount = lu.out_after;
                });
        } else {
            out_leasing_updates.insert(
                address.clone(),
                OutLeasingUpdate {
                    address: address.clone(),
                    new_amount: lu.out_after,
                },
            );
        }
    });

    out_leasing_updates.into_values().collect_vec()
}

fn handle_out_leasing_updates<R: repo::Repo>(
    repo: Arc<R>,
    updates: &[(&i64, OutLeasingUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let out_leasings_next_uid = repo.get_next_out_leasings_uid()?;

    let out_leasings_updates = updates
        .iter()
        .enumerate()
        .map(|(update_idx, (block_uid, update))| InsertableOutLeasing {
            uid: out_leasings_next_uid + update_idx as i64,
            superseded_by: -1,
            block_uid: *block_uid.clone(),
            address: update.address.clone(),
            amount: update.new_amount,
        })
        .collect_vec();

    let mut out_leasings_grouped: HashMap<InsertableOutLeasing, Vec<InsertableOutLeasing>> =
        HashMap::new();

    out_leasings_updates.into_iter().for_each(|update| {
        let group = out_leasings_grouped.entry(update.clone()).or_insert(vec![]);
        group.push(update);
    });

    let out_leasings_grouped = out_leasings_grouped.into_iter().collect_vec();

    let out_leasings_grouped_with_uids_superseded_by = out_leasings_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableOutLeasing>>();

            let mut last_uid = std::i64::MAX - 1;
            (
                group_key,
                updates
                    .as_mut_slice()
                    .iter_mut()
                    .rev()
                    .map(|cur| {
                        cur.superseded_by = last_uid;
                        last_uid = cur.uid;
                        cur.to_owned()
                    })
                    .sorted_by_key(|item| item.uid)
                    .collect(),
            )
        })
        .collect::<Vec<(InsertableOutLeasing, Vec<InsertableOutLeasing>)>>();

    let out_leasings_first_uids: Vec<OutLeasingOverride> =
        out_leasings_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                OutLeasingOverride {
                    superseded_by: first.uid,
                    address: first.address,
                }
            })
            .collect();

    repo.close_out_leasings_superseded_by(&out_leasings_first_uids)?;

    let out_leasings_with_uids_superseded_by = &out_leasings_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|issuer_balance| issuer_balance.uid)
        .collect_vec();

    repo.insert_out_leasings(out_leasings_with_uids_superseded_by)?;

    repo.set_out_leasings_next_update_uid(out_leasings_next_uid + updates_count as i64)
}

fn squash_microblocks<R: repo::Repo>(storage: Arc<R>) -> Result<()> {
    let total_block_id = storage.get_total_block_id()?;

    match total_block_id {
        Some(total_block_id) => {
            let key_block_uid = storage.get_key_block_uid()?;

            storage.update_assets_block_references(&key_block_uid)?;

            storage.update_data_entries_block_references(&key_block_uid)?;

            storage.update_issuer_balances_block_references(&key_block_uid)?;

            storage.update_out_leasings_block_references(&key_block_uid)?;

            storage.delete_microblocks()?;

            storage.change_block_id(&key_block_uid, &total_block_id)?;
        }
        None => (),
    }

    Ok(())
}

fn rollback<R, CBD, CUDD>(
    repo: Arc<R>,
    blockchain_data_cache: CBD,
    user_defined_data_cache: CUDD,
    waves_association_address: &str,
    block_uid: i64,
) -> Result<()>
where
    R: repo::Repo,
    CBD: SyncReadCache<AssetBlockchainData> + SyncWriteCache<AssetBlockchainData> + Clone,
    CUDD: SyncReadCache<AssetUserDefinedData> + SyncWriteCache<AssetUserDefinedData> + Clone,
{
    debug!("rollbacking to block_uid = {}", block_uid);

    // which assets have to be updated after rollback
    let assets_to_rollback = repo.assets_gt_block_uid(&block_uid)?;

    rollback_assets(repo.clone(), block_uid)?;

    rollback_data_entries(repo.clone(), block_uid)?;

    rollback_issuer_balances(repo.clone(), block_uid)?;

    rollback_out_leasings(repo.clone(), block_uid)?;

    repo.rollback_blocks_microblocks(&block_uid)?;

    let assets = repo.mget_assets(&assets_to_rollback)?;
    let asset_ids = assets
        .iter()
        .filter_map(|o| match o {
            Some(a) => Some(a.id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();

    let asset_oracles_data =
        repo.assets_oracle_data_entries(&asset_ids, waves_association_address)?;

    let assets_oracles_data =
        asset_oracles_data
            .into_iter()
            .fold(HashMap::new(), |mut acc, cur| {
                let asset_data = acc.entry(cur.asset_id.clone()).or_insert(HashMap::new());
                let asset_oracle_data = asset_data
                    .entry(cur.oracle_address.clone())
                    .or_insert(vec![]);
                asset_oracle_data.push(cur);
                acc
            });

    assets
        .into_iter()
        .filter_map(|o| match o {
            Some(a) => {
                let asset_oracles_data =
                    assets_oracles_data.get(&a.id).cloned().unwrap_or_default();
                let asset_oracles_data = asset_oracles_data
                    .into_iter()
                    .map(|(oracle_address, entries)| {
                        (
                            oracle_address,
                            entries
                                .iter()
                                .map(|e| AssetOracleDataEntry::from(e))
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<HashMap<String, Vec<AssetOracleDataEntry>>>();

                Some(AssetBlockchainData::from((&a, &asset_oracles_data)))
            }
            _ => None,
        })
        .try_for_each(|asset_blockchain_data| {
            blockchain_data_cache.set(&asset_blockchain_data.id.clone(), asset_blockchain_data)
        })?;

    // todo: invalidate labels (user defined data)

    Ok(())
}

fn rollback_assets<R: repo::Repo>(repo: Arc<R>, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_assets(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedAsset, Vec<DeletedAsset>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_assets_superseded_by(&lowest_deleted_uids)
}

fn rollback_data_entries<R: repo::Repo>(repo: Arc<R>, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_data_entries(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedDataEntry, Vec<DeletedDataEntry>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_data_entries_superseded_by(&lowest_deleted_uids)
}

fn rollback_issuer_balances<R: repo::Repo>(repo: Arc<R>, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_issuer_balances(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedIssuerBalance, Vec<DeletedIssuerBalance>> =
        HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_issuer_balances_superseded_by(&lowest_deleted_uids)
}

fn rollback_out_leasings<R: repo::Repo>(repo: Arc<R>, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_out_leasings(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedOutLeasing, Vec<DeletedOutLeasing>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_out_leasings_superseded_by(&lowest_deleted_uids)
}

fn escape_unicode_null(s: &str) -> String {
    s.replace("\0", "\\0")
}

fn parse_related_asset_id(key: &str) -> Option<String> {
    ASSET_ORACLE_DATA_ENTRY_KEY_REGEX
        .captures(key)
        .and_then(|cs| cs.get(1).map(|m| m.as_str().to_owned()))
}

impl From<&models::data_entry::DataEntryUpdate> for Option<AssetOracleDataEntry> {
    fn from(v: &models::data_entry::DataEntryUpdate) -> Self {
        v.related_asset_id.as_ref().and_then(|related_asset_id| {
            let (data_type, bin_val, bool_val, int_val, str_val) = match &v.value {
                Some(DataEntryValue::BinVal(v)) => (
                    Some(DataEntryValueType::Bin),
                    Some(v.to_owned()),
                    None,
                    None,
                    None,
                ),
                Some(DataEntryValue::BoolVal(v)) => (
                    Some(DataEntryValueType::Bool),
                    None,
                    Some(v.to_owned()),
                    None,
                    None,
                ),
                Some(DataEntryValue::IntVal(v)) => (
                    Some(DataEntryValueType::Int),
                    None,
                    None,
                    Some(v.to_owned()),
                    None,
                ),
                Some(DataEntryValue::StrVal(v)) => (
                    Some(DataEntryValueType::Str),
                    None,
                    None,
                    None,
                    Some(v.to_owned()),
                ),
                None => (None, None, None, None, None),
            };

            data_type.map(|data_type| AssetOracleDataEntry {
                asset_id: related_asset_id.to_owned(),
                oracle_address: v.address.to_owned(),
                key: v.key.to_owned(),
                data_type: DataEntryType::from(&data_type),
                bin_val,
                bool_val,
                int_val,
                str_val,
            })
        })
    }
}

fn is_asset_label_data_entry(key: &str, asset_id: &str) -> bool {
    *key == format!("status_<{}>", asset_id)
}

/// Extracts AssetLabelUpdate for WaVerified
///
/// At this moment it should be WaVerified label only,
/// but in the future it can be WaVerified | CommunityVerified labels depends on oracle address
fn extract_wa_verified_asset_label_update(
    oracles_data: &HashMap<String, Vec<AssetOracleDataEntry>>,
) -> Option<AssetLabelUpdate> {
    oracles_data.iter().fold(None, |_, (_oracle_address, des)| {
        des.iter().fold(None, |_, de| {
            if is_asset_label_data_entry(&de.key, &de.asset_id) {
                match de.int_val {
                    Some(verification_status) => {
                        if verification_status == ASSET_ORACLE_VERIFICATION_STATUS_VERIFIED {
                            // there is update, set new label
                            Some(AssetLabelUpdate::SetLabel(AssetLabel::WaVerified))
                        } else {
                            // there is update, unset label
                            Some(AssetLabelUpdate::DeleteLabel(AssetLabel::WaVerified))
                        }
                    }
                    // there is no update
                    _ => None,
                }
            } else {
                None
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        consumer::ASSET_ORACLE_VERIFICATION_STATUS_VERIFIED,
        models::{AssetLabel, AssetOracleDataEntry, DataEntryType},
    };

    use super::{
        escape_unicode_null, extract_wa_verified_asset_label_update, is_asset_label_data_entry,
        parse_related_asset_id, AssetLabelUpdate,
    };

    #[test]
    fn should_escape_unicode_null() {
        assert!("asd\0".contains("\0"));
        assert_eq!(escape_unicode_null("asd\0"), "asd\\0");
    }

    #[test]
    fn should_parse_related_asset_id() {
        let test_cases = vec![
            (
                "link_<9sQutD5HnRvjM1uui5cVC4w9xkMPAfYEV8ymug3Mon2Y>",
                Some("9sQutD5HnRvjM1uui5cVC4w9xkMPAfYEV8ymug3Mon2Y".to_owned()),
            ),
            (
                "description_<en>_<9sQutD5HnRvjM1uui5cVC4w9xkMPAfYEV8ymug3Mon2Y>",
                Some("9sQutD5HnRvjM1uui5cVC4w9xkMPAfYEV8ymug3Mon2Y".to_owned()),
            ),
            ("test", None),
        ];

        test_cases.into_iter().for_each(|(key, expected)| {
            let asset_id = parse_related_asset_id(key);
            assert_eq!(asset_id, expected);
        });
    }

    #[test]
    fn should_check_asset_label_data_entry() {
        let asset_id = "asset_id";

        let invalid_key = "asd";
        assert!(!is_asset_label_data_entry(invalid_key, asset_id));

        let valid_key = format!("status_<{}>", asset_id);
        assert!(is_asset_label_data_entry(&valid_key, asset_id));
    }

    #[test]
    fn should_extract_asset_label_update() {
        let asset_id = "asset_id".to_owned();
        let oracle_address = "oracle_address".to_owned();

        let set_label_de = AssetOracleDataEntry {
            asset_id: asset_id.clone(),
            oracle_address: oracle_address.clone(),
            key: format!("status_<{}>", asset_id),
            data_type: DataEntryType::Int,
            bin_val: None,
            bool_val: None,
            int_val: Some(ASSET_ORACLE_VERIFICATION_STATUS_VERIFIED),
            str_val: None,
        };

        let reset_label_de = AssetOracleDataEntry {
            asset_id: asset_id.clone(),
            oracle_address: oracle_address.clone(),
            key: format!("status_<{}>", asset_id),
            data_type: DataEntryType::Int,
            bin_val: None,
            bool_val: None,
            int_val: Some(3),
            str_val: None,
        };

        let empty_de = AssetOracleDataEntry {
            asset_id: asset_id.clone(),
            oracle_address: oracle_address.clone(),
            key: "bool".to_owned(),
            data_type: DataEntryType::Bool,
            bin_val: None,
            bool_val: Some(true),
            int_val: None,
            str_val: None,
        };

        // will set label
        let oracles_data = vec![(
            oracle_address.clone(),
            vec![
                empty_de.clone(),
                reset_label_de.clone(),
                set_label_de.clone(),
            ],
        )]
        .into_iter()
        .collect::<HashMap<String, Vec<AssetOracleDataEntry>>>();

        assert!(matches!(
            extract_wa_verified_asset_label_update(&oracles_data),
            Some(AssetLabelUpdate::SetLabel(AssetLabel::WaVerified))
        ));

        // will not set label
        let oracles_data = vec![(
            oracle_address.clone(),
            vec![
                empty_de.clone(),
                set_label_de.clone(),
                reset_label_de.clone(),
            ],
        )]
        .into_iter()
        .collect::<HashMap<String, Vec<AssetOracleDataEntry>>>();

        assert!(matches!(
            extract_wa_verified_asset_label_update(&oracles_data),
            Some(AssetLabelUpdate::DeleteLabel(AssetLabel::WaVerified))
        ));

        // there is no label update
        let oracles_data = vec![(
            oracle_address.clone(),
            vec![empty_de.clone(), empty_de.clone(), empty_de],
        )]
        .into_iter()
        .collect::<HashMap<String, Vec<AssetOracleDataEntry>>>();

        assert!(matches!(
            extract_wa_verified_asset_label_update(&oracles_data),
            None
        ));
    }
}
