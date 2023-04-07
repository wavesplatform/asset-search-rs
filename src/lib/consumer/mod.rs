pub mod models;
pub mod repo;
pub mod updates;

use anyhow::{Error, Result};
use bigdecimal::ToPrimitive;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use fragstrings::frag_parse;
use itertools::Itertools;
use repo::{Repo, RepoOperations};
use std::collections::{HashMap, HashSet};
use std::str;
use std::time::Instant;
use tokio::sync::mpsc::Receiver;
use waves_protobuf_schemas::waves::{
    data_transaction_data::data_entry::Value,
    events::{StateUpdate, TransactionMetadata},
    signed_transaction::Transaction,
    SignedTransaction, Transaction as WavesTx,
};
use wavesexchange_log::{debug, info, timer};

use self::models::asset::{AssetOverride, DeletedAsset, InsertableAsset};
use self::models::asset_descriptions::{
    AssetDescriptionOverride, DeletedAssetDescription, InsertableAssetDescription,
};
use self::models::asset_labels::{AssetLabelsOverride, DeletedAssetLabels, InsertableAssetLabels};
use self::models::asset_names::{AssetNameOverride, DeletedAssetName, InsertableAssetName};
use self::models::asset_tickers::{AssetTickerOverride, DeletedAssetTicker, InsertableAssetTicker};
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
use crate::models::{AssetOracleDataEntry, BaseAssetInfoUpdate, DataEntryType};
use crate::services::assets::repo::UserDefinedData;
use crate::waves::{
    get_asset_id, is_waves_asset_id, parse_waves_association_key, Address,
    KNOWN_WAVES_ASSOCIATION_ASSET_ATTRIBUTES, WAVES_ID,
};

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
pub struct AssetLabelsUpdate {
    pub asset_id: String,
    pub labels: Vec<String>,
}

#[derive(Debug)]
pub struct AssetTickerUpdate {
    pub asset_id: String,
    pub ticker: String,
}

#[derive(Debug)]
pub struct AssetNameUpdate {
    pub asset_id: String,
    pub asset_name: String,
}

#[derive(Debug)]
pub struct AssetDescriptionUpdate {
    pub asset_id: String,
    pub asset_description: String,
}

#[derive(Clone, Debug)]
pub enum AssetLabelUpdate {
    SetLabel(String),
    DeleteLabel(String),
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
    repo: &R,
    blockchain_data_cache: &CBD,
    user_defined_data_cache: &CUDD,
    updates_per_request: usize,
    max_wait_time_in_secs: u64,
    chain_id: u8,
    asset_storage_address: String,
    start_rollback_depth: u32,
) -> Result<()>
where
    T: UpdatesSource + Send + Sync + 'static,
    R: Repo,
    CBD: SyncReadCache<AssetBlockchainData>
        + SyncWriteCache<AssetBlockchainData>
        + Clone
        + Send
        + Sync
        + 'static,
    CUDD: SyncReadCache<AssetUserDefinedData>
        + SyncWriteCache<AssetUserDefinedData>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let prev_h = repo
        .execute(move |o| o.get_prev_handled_height(start_rollback_depth))
        .await?;

    if let Some(prev_h) = prev_h.as_ref() {
        info!(
            "Rolling back to height {} (by {} blocks back)",
            prev_h.height, start_rollback_depth
        );

        let last_uid = prev_h.uid;
        repo.transaction(move |o| rollback(o, last_uid)).await?;
    }

    let starting_from_height = match prev_h {
        Some(prev_handled_height) => prev_handled_height.height as u32 + 1,
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

        let wwa = asset_storage_address.clone();

        start = Instant::now();

        let changed_assets_ids = repo
            .transaction(move |o| {
                Ok(handle_updates(
                    updates_with_height,
                    o,
                    chain_id.clone(),
                    wwa,
                )?)
            })
            .await?;

        update_redis_cache_from_db(
            repo,
            changed_assets_ids,
            blockchain_data_cache,
            user_defined_data_cache,
            asset_storage_address.clone(),
        )
        .await?;

        info!(
            "{} updates were handled in {:?} ms. Last updated height is {}.",
            updates_count,
            start.elapsed().as_millis(),
            &last_height
        );
    }
}

fn handle_updates<'a, R>(
    updates_with_height: BlockchainUpdatesWithLastHeight,
    repo: &R,
    chain_id: u8,
    asset_storage_address: String,
) -> Result<Vec<String>>
where
    R: repo::RepoOperations,
{
    let mut upd = vec![];
    updates_with_height
        .updates
        .into_iter()
        .fold::<&mut Vec<UpdatesItem>, _>(&mut upd, |acc, cur| match cur {
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
        });

    let mut acc_changed_ids = vec![];

    for update_item in upd {
        match update_item {
            UpdatesItem::Blocks(bs) => {
                squash_microblocks(repo)?;
                let mut changed_asset_ids =
                    handle_appends(repo, chain_id, bs.as_ref(), asset_storage_address.clone())?;
                acc_changed_ids.append(&mut changed_asset_ids);
            }
            UpdatesItem::Microblock(mba) => {
                let mut changed_asset_ids = handle_appends(
                    repo,
                    chain_id,
                    &vec![mba.to_owned()],
                    asset_storage_address.clone(),
                )?;
                acc_changed_ids.append(&mut changed_asset_ids);
            }
            UpdatesItem::Rollback(sig) => {
                let block_uid = repo.clone().get_block_uid(&sig)?;
                let mut changed_asset_ids = rollback(repo.clone(), block_uid)?;
                acc_changed_ids.append(&mut changed_asset_ids);
            }
        }
    }

    Ok(acc_changed_ids)
}

fn handle_appends<'a, R>(
    repo: &R,
    chain_id: u8,
    appends: &Vec<BlockMicroblockAppend>,
    asset_storage_address: String,
) -> Result<Vec<String>>
where
    R: repo::RepoOperations,
{
    let mut changed_issuers: Vec<String> = vec![];
    let mut changed_asset_ids: Vec<String> = vec![];

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

    // Handle base asset info updates
    let base_asset_info_updates_with_block_uids = {
        timer!("assets updates handling");

        let base_asset_info_updates_with_block_uids: Vec<(&i64, BaseAssetInfoUpdate)> =
            block_uids_with_appends
                .iter()
                .flat_map(|(block_uid, append)| {
                    extract_base_asset_info_updates(chain_id, append)
                        .into_iter()
                        .map(|au| {
                            changed_asset_ids.push(au.id.clone());
                            (block_uid, au)
                        })
                        .collect_vec()
                })
                .collect();

        handle_base_asset_info_updates(repo, &base_asset_info_updates_with_block_uids)?;

        info!(
            "handled {} assets updates",
            base_asset_info_updates_with_block_uids.len()
        );

        base_asset_info_updates_with_block_uids
    };

    // Handle data entries updates
    timer!("data entries updates handling");

    let data_entries_updates_with_block_uids: Vec<(&i64, DataEntryUpdate)> =
        block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                append
                    .txs
                    .iter()
                    .flat_map(|tx| {
                        extract_asset_related_data_entries_updates(
                            append.height as i32,
                            tx,
                            asset_storage_address.clone(),
                        )
                    })
                    .map(|u| {
                        if u.related_asset_id.is_some() {
                            changed_asset_ids.push(u.related_asset_id.as_ref().unwrap().clone());
                        }
                        (block_uid, u)
                    })
                    .collect_vec()
            })
            .collect();

    handle_asset_related_data_entries_updates(repo, &data_entries_updates_with_block_uids)?;

    info!(
        "handled {} data entries updates",
        data_entries_updates_with_block_uids.len()
    );

    // Handle asset labels updates
    timer!("asset label updates handling");

    let asset_labels_updates_with_block_uids: Vec<(&i64, AssetLabelsUpdate)> =
        block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                append
                    .txs
                    .iter()
                    .flat_map(|tx| {
                        extract_asset_labels_updates(
                            append.height as i32,
                            tx,
                            asset_storage_address.clone(),
                        )
                    })
                    .map(|u| (block_uid, u))
                    .collect_vec()
            })
            .collect();

    handle_asset_labels_updates(repo, &asset_labels_updates_with_block_uids)?;

    asset_labels_updates_with_block_uids.iter().for_each(|i| {
        changed_asset_ids.push(i.1.asset_id.clone());
    });

    info!(
        "handled {} asset label updates",
        asset_labels_updates_with_block_uids.len()
    );

    // Handle asset tickers updates
    timer!("asset tickers updates handling");

    let asset_tickers_updates_with_block_uids: Vec<(&i64, AssetTickerUpdate)> =
        block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                append
                    .txs
                    .iter()
                    .flat_map(|tx| {
                        extract_asset_tickers_updates(
                            append.height as i32,
                            tx,
                            asset_storage_address.clone(),
                        )
                    })
                    .map(|u| {
                        changed_asset_ids.push(u.asset_id.clone());
                        (block_uid, u)
                    })
                    .collect_vec()
            })
            .collect();

    handle_asset_tickers_updates(repo, &asset_tickers_updates_with_block_uids)?;

    info!(
        "handled {} asset tickers updates",
        asset_tickers_updates_with_block_uids.len()
    );

    // Handle asset names updates
    timer!("asset names updates handling");

    let asset_names_updates_with_block_uids: Vec<(&i64, AssetNameUpdate)> = block_uids_with_appends
        .iter()
        .flat_map(|(block_uid, append)| {
            append
                .txs
                .iter()
                .flat_map(|tx| {
                    extract_asset_name_updates(
                        append.height as i32,
                        tx,
                        asset_storage_address.clone(),
                    )
                })
                .map(|u| {
                    changed_asset_ids.push(u.asset_id.clone());
                    (block_uid, u)
                })
                .collect_vec()
        })
        .collect();

    handle_asset_names_updates(repo, &asset_names_updates_with_block_uids)?;

    info!(
        "handled {} asset names updates",
        asset_names_updates_with_block_uids.len()
    );

    // Handle asset descriptions updates
    timer!("asset descriptions updates handling");

    let asset_descriptions_updates_with_block_uids: Vec<(&i64, AssetDescriptionUpdate)> =
        block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                append
                    .txs
                    .iter()
                    .flat_map(|tx| {
                        extract_asset_description_updates(
                            append.height as i32,
                            tx,
                            asset_storage_address.clone(),
                        )
                    })
                    .map(|u| {
                        changed_asset_ids.push(u.asset_id.clone());
                        (block_uid, u)
                    })
                    .collect_vec()
            })
            .collect();

    handle_asset_descriptions_updates(repo, &asset_descriptions_updates_with_block_uids)?;

    info!(
        "handled {} asset descriptions updates",
        asset_descriptions_updates_with_block_uids.len()
    );

    // Handle issuer balances updates
    timer!("issuer balances updates handling");

    let current_issuer_balances = repo.get_current_issuer_balances()?;

    let issuers = base_asset_info_updates_with_block_uids
        .iter()
        .filter(|(_, au)| au.id != WAVES_ID)
        .map(|(_, au)| {
            changed_asset_ids.push(au.id.clone());
            au.issuer.as_ref()
        })
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
                extract_issuers_balance_updates(&append, &issuers)
                    .into_iter()
                    .map(|u| {
                        changed_issuers.push(u.address.clone());
                        (block_uid, u)
                    })
                    .collect_vec()
            })
            .collect();

    handle_issuer_balances_updates(repo, &issuer_balances_updates_with_block_uids)?;

    info!(
        "handled {} issuer balances updates",
        issuer_balances_updates_with_block_uids.len()
    );

    // Handle out leasing updates
    timer!("out leasing updates handling");

    let out_leasing_updates_with_block_uids: Vec<(&i64, OutLeasingUpdate)> =
        block_uids_with_appends
            .iter()
            .flat_map(|(block_uid, append)| {
                extract_out_leasing_updates(&append)
                    .into_iter()
                    .map(|u| (block_uid, u))
                    .collect_vec()
            })
            .collect();

    handle_out_leasing_updates(repo, &out_leasing_updates_with_block_uids)?;

    out_leasing_updates_with_block_uids
        .iter()
        .for_each(|l| changed_issuers.push(l.1.address.clone()));

    let mut issuers_assets = repo.get_last_asset_ids_by_issuers(&changed_issuers)?;
    changed_asset_ids.append(&mut issuers_assets);

    info!(
        "handled {} out leasing updates",
        out_leasing_updates_with_block_uids.len()
    );

    base_asset_info_updates_with_block_uids
        .iter()
        .for_each(|a| changed_asset_ids.push(a.1.id.clone()));

    changed_asset_ids.sort();
    changed_asset_ids.dedup();

    Ok(changed_asset_ids)
}

async fn update_redis_cache_from_db<'a, R, CBD, CUDD>(
    repo: &R,
    asset_ids: Vec<String>,
    blockchain_data_cache: &CBD,
    user_defined_data_cache: &CUDD,
    asset_storage_address: String,
) -> Result<()>
where
    R: repo::Repo,
    CBD: SyncReadCache<AssetBlockchainData> + SyncWriteCache<AssetBlockchainData> + Clone,
    CUDD: SyncReadCache<AssetUserDefinedData> + SyncWriteCache<AssetUserDefinedData> + Clone,
{
    let asset_ids_copy = asset_ids.clone();

    let assets_oracles_data = repo
        .execute(move |o| o.data_entries(&asset_ids_copy, asset_storage_address.clone()))
        .await?;

    let assets_oracles_data =
        assets_oracles_data
            .into_iter()
            .fold(HashMap::new(), |mut acc, cur| {
                let asset_data = acc.entry(cur.asset_id.clone()).or_insert(HashMap::new());
                let asset_oracle_data = asset_data
                    .entry(cur.oracle_address.clone())
                    .or_insert(vec![]);
                let asset_oracle_data_entry = AssetOracleDataEntry::from(&cur);
                asset_oracle_data.push(asset_oracle_data_entry);
                acc
            });

    let mut assets_user_defined_data: HashMap<String, UserDefinedData> = HashMap::new();

    let asset_ids_copy = asset_ids.clone();
    let asset_user_defined_data = repo
        .execute(move |o| o.mget_asset_user_defined_data(&asset_ids_copy))
        .await?;

    asset_user_defined_data.into_iter().for_each(|d| {
        assets_user_defined_data.insert(d.asset_id.clone(), d);
    });

    let asset_ids_copy = asset_ids.clone();

    let assets = repo
        .execute(move |o| o.mget_assets_by_ids(&asset_ids_copy))
        .await?;

    assets.into_iter().filter(|i| i.is_some()).for_each(|i| {
        let i = i.unwrap();

        let oracle_data = assets_oracles_data
            .get(&i.id)
            .unwrap_or(&HashMap::new())
            .clone();

        let base = AssetBlockchainData::from_asset_and_oracles_data(&i, &oracle_data);

        let udd = assets_user_defined_data.get(&base.id);

        if let Some(udd) = udd {
            user_defined_data_cache
                .set(&udd.asset_id.clone(), udd.into())
                .expect("can't set asset user defined data in redis");
        }

        blockchain_data_cache
            .set(&base.id.clone(), base)
            .expect("can't set asset data in redis");
    });

    debug!("{} updated redis cache", &asset_ids.len());

    Ok(())
}

fn extract_base_asset_info_updates(
    chain_id: u8,
    append: &BlockMicroblockAppend,
) -> Vec<BaseAssetInfoUpdate> {
    let mut asset_updates = vec![];

    let update_time_stamp = match append.time_stamp {
        Some(time_stamp) => DateTime::from_utc(
            NaiveDateTime::from_timestamp_opt(time_stamp / 1000, time_stamp as u32 % 1000 * 1000)
                .expect("invalid timestamp data"),
            Utc,
        ),
        None => Utc::now(),
    };

    if let Some(updated_waves_amount) = append.updated_waves_amount {
        asset_updates.push(BaseAssetInfoUpdate::waves_update(
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
                        let time_stamp = match tx.data.transaction.as_ref() {
                            Some(stx) => match stx {
                                Transaction::WavesTransaction(WavesTx { timestamp, .. }) => {
                                    DateTime::from_utc(
                                        NaiveDateTime::from_timestamp_opt(
                                            timestamp / 1000,
                                            *timestamp as u32 % 1000 * 1000,
                                        )
                                        .expect("invalid timestamp data"),
                                        Utc,
                                    )
                                }
                                Transaction::EthereumTransaction(_) => return None,
                            },
                            _ => Utc::now(),
                        };

                        let asset_id = get_asset_id(&asset_details.asset_id);
                        let issuer =
                            Address::from((asset_details.issuer.as_slice(), chain_id)).into();
                        Some(BaseAssetInfoUpdate {
                            update_height: append.height as i32,
                            updated_at: time_stamp,
                            id: asset_id,
                            name: escape_unicode_null(&asset_details.name),
                            description: escape_unicode_null(&asset_details.description),
                            issuer,
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

fn handle_base_asset_info_updates<R: repo::RepoOperations>(
    repo: &R,
    updates: &[(&i64, BaseAssetInfoUpdate)],
) -> Result<()> {
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
            time_stamp: update.updated_at,
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

fn extract_asset_related_data_entries_updates(
    height: i32,
    tx: &Tx,
    asset_storage_address: String,
) -> Vec<DataEntryUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            let transaction = match tx.data.transaction.as_ref() {
                Some(Transaction::WavesTransaction(wtx)) => wtx,
                Some(Transaction::EthereumTransaction(_)) | None => return None,
            };
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if asset_storage_address == oracle_address {
                    let parsed_key = parse_waves_association_key(
                        &KNOWN_WAVES_ASSOCIATION_ASSET_ATTRIBUTES,
                        &de.key,
                    );
                    let time_stamp = DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt(transaction.timestamp / 1000, 0)
                            .expect("invalid timestamp data"),
                        Utc,
                    );

                    Some(DataEntryUpdate {
                        update_height: height,
                        updated_at: time_stamp,
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
                        related_asset_id: parsed_key.map(|k| k.asset_id),
                    })
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

fn handle_asset_related_data_entries_updates<R: repo::RepoOperations>(
    repo: &R,
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

fn extract_asset_name_updates(
    _height: i32,
    tx: &Tx,
    asset_storage_address: String,
) -> Vec<AssetNameUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if asset_storage_address == oracle_address && is_asset_name_data_entry(&de.key) {
                    match de.value.as_ref() {
                        Some(value) => match value {
                            Value::StringValue(value)
                                if asset_storage_address == oracle_address =>
                            {
                                frag_parse!("%s%s", de.key).map(|(_, asset_id)| AssetNameUpdate {
                                    asset_id: asset_id,
                                    asset_name: value.clone(),
                                })
                            }
                            _ => None,
                        },
                        // key was deleted -> drop asset ticker
                        None => frag_parse!("%s%s", de.key).map(|(_, asset_id)| AssetNameUpdate {
                            asset_id,
                            asset_name: "".into(),
                        }),
                    }
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

fn extract_asset_description_updates(
    _height: i32,
    tx: &Tx,
    asset_storage_address: String,
) -> Vec<AssetDescriptionUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if asset_storage_address == oracle_address
                    && is_asset_description_data_entry(&de.key)
                {
                    match de.value.as_ref() {
                        Some(value) => match value {
                            Value::StringValue(value)
                                if asset_storage_address == oracle_address =>
                            {
                                frag_parse!("%s%s", de.key).map(|(_, asset_id)| {
                                    AssetDescriptionUpdate {
                                        asset_id: asset_id,
                                        asset_description: value.clone(),
                                    }
                                })
                            }
                            _ => None,
                        },
                        // key was deleted -> drop asset ticker
                        None => frag_parse!("%s%s", de.key).map(|(_, asset_id)| {
                            AssetDescriptionUpdate {
                                asset_id,
                                asset_description: "".into(),
                            }
                        }),
                    }
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

fn extract_asset_tickers_updates(
    _height: i32,
    tx: &Tx,
    asset_storage_address: String,
) -> Vec<AssetTickerUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if asset_storage_address == oracle_address && is_asset_ticker_data_entry(&de.key) {
                    match de.value.as_ref() {
                        Some(value) => match value {
                            Value::StringValue(value)
                                if asset_storage_address == oracle_address =>
                            {
                                frag_parse!("%s%s", de.key).map(|(_, asset_id)| AssetTickerUpdate {
                                    asset_id: asset_id,
                                    ticker: value.clone(),
                                })
                            }
                            _ => None,
                        },
                        // key was deleted -> drop asset ticker
                        None => {
                            frag_parse!("%s%s", de.key).map(|(_, asset_id)| AssetTickerUpdate {
                                asset_id,
                                ticker: "".into(),
                            })
                        }
                    }
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

// https://confluence.wavesplatform.com/display/DEXPRODUCTS/WX+Governance#WXGovernance-%D0%9B%D0%B5%D0%B9%D0%B1%D0%BB%D1%8B%D0%B0%D1%81%D1%81%D0%B5%D1%82%D0%B0
fn extract_asset_labels_updates(
    _height: i32,
    tx: &Tx,
    asset_storage_address: String,
) -> Vec<AssetLabelsUpdate> {
    tx.state_update
        .data_entries
        .iter()
        .filter_map(|data_entry_update| {
            data_entry_update.data_entry.as_ref().and_then(|de| {
                let oracle_address = bs58::encode(&data_entry_update.address).into_string();
                if asset_storage_address == oracle_address && is_asset_labels_data_entry(&de.key) {
                    match de.value.as_ref() {
                        Some(value) => match value {
                            Value::StringValue(value)
                                if asset_storage_address == oracle_address =>
                            {
                                frag_parse!("%s%s", de.key).map(|(_, asset_id)| {
                                    let labels = parse_asset_labels(&value);
                                    AssetLabelsUpdate { asset_id, labels }
                                })
                            }
                            _ => None,
                        },
                        // key was deleted -> drop asset labels
                        None => {
                            frag_parse!("%s%s", de.key).map(|(_, asset_id)| AssetLabelsUpdate {
                                asset_id,
                                labels: vec![],
                            })
                        }
                    }
                } else {
                    None
                }
            })
        })
        .collect_vec()
}

fn handle_asset_labels_updates<R: repo::RepoOperations>(
    repo: &R,
    updates: &[(&i64, AssetLabelsUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let asset_labels_next_uid = repo.get_next_asset_labels_uid()?;

    let asset_labels_updates = updates
        .iter()
        .enumerate()
        .map(
            |(update_idx, (block_uid, labels_update))| InsertableAssetLabels {
                uid: asset_labels_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                asset_id: labels_update.asset_id.clone(),
                labels: labels_update.labels.clone(),
            },
        )
        .collect_vec();

    let mut asset_labels_grouped: HashMap<InsertableAssetLabels, Vec<InsertableAssetLabels>> =
        HashMap::new();

    asset_labels_updates.into_iter().for_each(|update| {
        let group = asset_labels_grouped.entry(update.clone()).or_insert(vec![]);
        group.push(update);
    });

    let asset_labels_grouped = asset_labels_grouped.into_iter().collect_vec();

    let asset_labels_grouped_with_uids_superseded_by = asset_labels_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableAssetLabels>>();

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
        .collect::<Vec<(InsertableAssetLabels, Vec<InsertableAssetLabels>)>>();

    let asset_labels_first_uids: Vec<AssetLabelsOverride> =
        asset_labels_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                AssetLabelsOverride {
                    superseded_by: first.uid,
                    asset_id: first.asset_id,
                }
            })
            .collect();

    repo.close_asset_labels_superseded_by(&asset_labels_first_uids)?;

    let asset_labels_with_uids_superseded_by = &asset_labels_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|asset_labels| asset_labels.uid)
        .collect_vec();

    repo.insert_asset_labels(asset_labels_with_uids_superseded_by)?;

    repo.set_asset_labels_next_update_uid(asset_labels_next_uid + updates_count as i64)
}

fn handle_asset_tickers_updates<R: repo::RepoOperations>(
    repo: &R,
    updates: &[(&i64, AssetTickerUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let asset_tickers_next_uid = repo.get_next_asset_tickers_uid()?;

    let asset_tickers_updates = updates
        .iter()
        .enumerate()
        .map(
            |(update_idx, (block_uid, tickers_update))| InsertableAssetTicker {
                uid: asset_tickers_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                asset_id: tickers_update.asset_id.clone(),
                ticker: tickers_update.ticker.clone(),
            },
        )
        .collect_vec();

    let mut asset_tickers_grouped: HashMap<InsertableAssetTicker, Vec<InsertableAssetTicker>> =
        HashMap::new();

    asset_tickers_updates.into_iter().for_each(|update| {
        let group = asset_tickers_grouped
            .entry(update.clone())
            .or_insert(vec![]);
        group.push(update);
    });

    let asset_tickers_grouped = asset_tickers_grouped.into_iter().collect_vec();

    let asset_tickers_grouped_with_uids_superseded_by = asset_tickers_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableAssetTicker>>();

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
        .collect::<Vec<(InsertableAssetTicker, Vec<InsertableAssetTicker>)>>();

    let asset_tickers_first_uids: Vec<AssetTickerOverride> =
        asset_tickers_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                AssetTickerOverride {
                    superseded_by: first.uid,
                    asset_id: first.asset_id,
                }
            })
            .collect();

    repo.close_asset_tickers_superseded_by(&asset_tickers_first_uids)?;

    let asset_tickers_with_uids_superseded_by = &asset_tickers_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|asset_tickers| asset_tickers.uid)
        .collect_vec();

    repo.insert_asset_tickers(asset_tickers_with_uids_superseded_by)?;

    repo.set_asset_tickers_next_update_uid(asset_tickers_next_uid + updates_count as i64)
}

fn handle_asset_names_updates<R: repo::RepoOperations>(
    repo: &R,
    updates: &[(&i64, AssetNameUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let asset_names_next_uid = repo.get_next_asset_names_uid()?;

    let asset_names_updates = updates
        .iter()
        .enumerate()
        .map(
            |(update_idx, (block_uid, name_update))| InsertableAssetName {
                uid: asset_names_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                asset_id: name_update.asset_id.clone(),
                asset_name: name_update.asset_name.clone(),
            },
        )
        .collect_vec();

    let mut asset_names_grouped: HashMap<InsertableAssetName, Vec<InsertableAssetName>> =
        HashMap::new();

    asset_names_updates.into_iter().for_each(|update| {
        let group = asset_names_grouped.entry(update.clone()).or_insert(vec![]);
        group.push(update);
    });

    let asset_names_grouped = asset_names_grouped.into_iter().collect_vec();

    let asset_names_grouped_with_uids_superseded_by = asset_names_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableAssetName>>();

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
        .collect::<Vec<(InsertableAssetName, Vec<InsertableAssetName>)>>();

    let asset_names_first_uids: Vec<AssetNameOverride> =
        asset_names_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                AssetNameOverride {
                    superseded_by: first.uid,
                    asset_id: first.asset_id,
                }
            })
            .collect();

    repo.close_asset_names_superseded_by(&asset_names_first_uids)?;

    let asset_names_with_uids_superseded_by = &asset_names_grouped_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|asset_names| asset_names.uid)
        .collect_vec();

    repo.insert_asset_names(asset_names_with_uids_superseded_by)?;

    repo.set_asset_tickers_next_update_uid(asset_names_next_uid + updates_count as i64)
}

fn handle_asset_descriptions_updates<R: repo::RepoOperations>(
    repo: &R,
    updates: &[(&i64, AssetDescriptionUpdate)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let updates_count = updates.len();

    let asset_descriptions_next_uid = repo.get_next_asset_descriptions_uid()?;

    let asset_descriptions_updates = updates
        .iter()
        .enumerate()
        .map(
            |(update_idx, (block_uid, description_update))| InsertableAssetDescription {
                uid: asset_descriptions_next_uid + update_idx as i64,
                superseded_by: -1,
                block_uid: *block_uid.clone(),
                asset_id: description_update.asset_id.clone(),
                asset_description: description_update.asset_description.clone(),
            },
        )
        .collect_vec();

    let mut asset_descriptions_grouped: HashMap<
        InsertableAssetDescription,
        Vec<InsertableAssetDescription>,
    > = HashMap::new();

    asset_descriptions_updates.into_iter().for_each(|update| {
        let group = asset_descriptions_grouped
            .entry(update.clone())
            .or_insert(vec![]);
        group.push(update);
    });

    let asset_descriptions_grouped = asset_descriptions_grouped.into_iter().collect_vec();

    let asset_descriptions_grouped_with_uids_superseded_by = asset_descriptions_grouped
        .into_iter()
        .map(|(group_key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableAssetDescription>>();

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
        .collect::<Vec<(InsertableAssetDescription, Vec<InsertableAssetDescription>)>>();

    let asset_descriptions_first_uids: Vec<AssetDescriptionOverride> =
        asset_descriptions_grouped_with_uids_superseded_by
            .iter()
            .map(|(_, group)| {
                let first = group.iter().next().unwrap().clone();
                AssetDescriptionOverride {
                    superseded_by: first.uid,
                    asset_id: first.asset_id,
                }
            })
            .collect();

    repo.close_asset_descriptions_superseded_by(&asset_descriptions_first_uids)?;

    let asset_descriptions_with_uids_superseded_by =
        &asset_descriptions_grouped_with_uids_superseded_by
            .clone()
            .into_iter()
            .flat_map(|(_, v)| v)
            .sorted_by_key(|asset_descriptions| asset_descriptions.uid)
            .collect_vec();

    repo.insert_asset_descriptions(asset_descriptions_with_uids_superseded_by)?;

    repo.set_asset_descriptions_next_update_uid(asset_descriptions_next_uid + updates_count as i64)
}

fn extract_issuers_balance_updates(
    append: &BlockMicroblockAppend,
    issuers: &HashSet<&str>,
) -> Vec<IssuerBalanceUpdate> {
    // at first, balance updates placed at append.state_update
    // at second, balance updates placed at append.txs[i].state_update
    // so balance updates from txs[i].state_update should override balance updates from append.state_update

    let mut issuer_balance_updates = HashMap::new();

    append
        .state_update
        .balances
        .iter()
        .map(|balance_update| (append.time_stamp, balance_update))
        .chain(append.txs.iter().flat_map(|tx| {
            tx.state_update
                .balances
                .iter()
                .map(move |balance_update| match tx.data.transaction {
                    Some(Transaction::WavesTransaction(WavesTx { timestamp, .. })) => {
                        (Some(timestamp), balance_update)
                    }
                    _ => (None, balance_update),
                })
        }))
        .filter_map(move |(time_stamp, balance_update)| {
            let address = bs58::encode(&balance_update.address).into_string();
            // handle issuers balances only
            if issuers.contains(&address.as_str()) {
                balance_update
                    .amount_after
                    .as_ref()
                    .and_then(|amount_after| {
                        // handle issuer waves balance changes only
                        if is_waves_asset_id(&amount_after.asset_id)
                            && balance_update.amount_before != amount_after.amount
                        {
                            let updated_at = match &time_stamp {
                                Some(timestamp) => DateTime::from_utc(
                                    NaiveDateTime::from_timestamp_opt(
                                        timestamp / 1000,
                                        *timestamp as u32 % 1000 * 1000,
                                    )
                                    .expect("invalid timestamp data"),
                                    Utc,
                                ),
                                _ => Utc::now(),
                            };

                            Some((address, amount_after.amount, updated_at, append.height))
                        } else {
                            None
                        }
                    })
            } else {
                None
            }
        })
        .for_each(|(address, amount_after, updated_at, update_height)| {
            issuer_balance_updates.insert(
                address.clone(),
                IssuerBalanceUpdate {
                    updated_at,
                    update_height: update_height as i32,
                    address,
                    new_regular_balance: amount_after,
                },
            );
        });

    issuer_balance_updates.into_values().collect_vec()
}

fn handle_issuer_balances_updates<R: repo::RepoOperations>(
    repo: &R,
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

fn extract_out_leasing_updates(append: &BlockMicroblockAppend) -> Vec<OutLeasingUpdate> {
    // at first, balance updates placed at append.state_update
    // at second, balance updates placed at append.txs[i].state_update
    // so balance updates from txs[i].state_update should override balance updates from append.state_update

    let mut out_leasing_updates = HashMap::new();

    append
        .state_update
        .leasing_for_address
        .clone()
        .iter()
        .chain(
            append
                .txs
                .iter()
                .flat_map(|tx| tx.state_update.leasing_for_address.iter()),
        )
        .for_each(|leasing_update| {
            // handle out leasing changes only
            if leasing_update.out_after != leasing_update.out_before {
                let updated_at = match append.time_stamp {
                    Some(time_stamp) => DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt(
                            time_stamp / 1000,
                            time_stamp as u32 % 1000 * 1000,
                        )
                        .expect("invalid timestamp data"),
                        Utc,
                    ),
                    _ => Utc::now(),
                };

                let address = bs58::encode(&leasing_update.address).into_string();

                if out_leasing_updates.contains_key(&address) {
                    out_leasing_updates.entry(address.clone()).and_modify(
                        |u: &mut OutLeasingUpdate| {
                            u.new_amount = leasing_update.out_after;
                        },
                    );
                } else {
                    out_leasing_updates.insert(
                        address.clone(),
                        OutLeasingUpdate {
                            updated_at,
                            update_height: append.height as i32,
                            address: address.clone(),
                            new_amount: leasing_update.out_after,
                        },
                    );
                }
            }
        });

    out_leasing_updates.into_values().collect_vec()
}

fn handle_out_leasing_updates<R: repo::RepoOperations>(
    repo: &R,
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

fn squash_microblocks<R: repo::RepoOperations>(storage: &R) -> Result<()> {
    let total_block_id = storage.get_total_block_id()?;

    match total_block_id {
        Some(total_block_id) => {
            let key_block_uid = storage.get_key_block_uid()?;

            storage.update_assets_block_references(&key_block_uid)?;

            storage.update_asset_labels_block_references(&key_block_uid)?;

            storage.update_asset_tickers_block_references(&key_block_uid)?;

            storage.update_data_entries_block_references(&key_block_uid)?;

            storage.update_issuer_balances_block_references(&key_block_uid)?;

            storage.update_out_leasings_block_references(&key_block_uid)?;

            storage.update_asset_names_block_references(&key_block_uid)?;

            storage.update_asset_descriptions_block_references(&key_block_uid)?;

            storage.delete_microblocks()?;

            storage.change_block_id(&key_block_uid, &total_block_id)?;
        }
        None => (),
    }

    Ok(())
}

fn rollback<R>(repo: &R, block_uid: i64) -> Result<Vec<String>>
where
    R: repo::RepoOperations,
{
    debug!("rollbacking to block_uid = {}", block_uid);

    // which assets have to be updated after rollback
    let assets_to_rollback = repo.assets_gt_block_uid(&block_uid)?;

    rollback_assets(repo, block_uid)?;

    rollback_asset_labels(repo, block_uid)?;

    rollback_asset_tickers(repo, block_uid)?;

    rollback_data_entries(repo, block_uid)?;

    rollback_issuer_balances(repo, block_uid)?;

    rollback_out_leasings(repo, block_uid)?;

    rollback_asset_names(repo, block_uid)?;

    rollback_asset_descriptions(repo, block_uid)?;

    repo.rollback_blocks_microblocks(&block_uid)?;

    // Invalidate cache
    let assets_ids = repo.mget_assets(&assets_to_rollback)?;
    let assets_ids = assets_ids
        .iter()
        .filter(|i| i.is_some())
        .map(|i| i.as_ref().unwrap().id.clone())
        .collect();

    Ok(assets_ids)
}

fn rollback_assets<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
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

fn rollback_asset_labels<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_asset_labels(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedAssetLabels, Vec<DeletedAssetLabels>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_asset_labels_superseded_by(&lowest_deleted_uids)
}

fn rollback_asset_tickers<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_asset_tickers(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedAssetTicker, Vec<DeletedAssetTicker>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_asset_tickers_superseded_by(&lowest_deleted_uids)
}

fn rollback_data_entries<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
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

fn rollback_issuer_balances<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
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

fn rollback_out_leasings<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
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

fn rollback_asset_names<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_asset_names(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedAssetName, Vec<DeletedAssetName>> = HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_asset_names_superseded_by(&lowest_deleted_uids)
}

fn rollback_asset_descriptions<R: repo::RepoOperations>(repo: &R, block_uid: i64) -> Result<()> {
    let deleted = repo.rollback_asset_descriptions(&block_uid)?;

    let mut grouped_deleted: HashMap<DeletedAssetDescription, Vec<DeletedAssetDescription>> =
        HashMap::new();

    deleted.into_iter().for_each(|item| {
        let group = grouped_deleted.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deleted
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo.reopen_asset_descriptions_superseded_by(&lowest_deleted_uids)
}

fn escape_unicode_null(s: &str) -> String {
    s.replace("\0", "\\0")
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

fn is_asset_labels_data_entry(key: &str) -> bool {
    key.starts_with("%s%s__labels__")
}

fn is_asset_ticker_data_entry(key: &str) -> bool {
    key.starts_with("%s%s__assetId2ticker__")
}

fn is_asset_name_data_entry(key: &str) -> bool {
    key.starts_with("%s%s__assetName__")
}

fn is_asset_description_data_entry(key: &str) -> bool {
    key.starts_with("%s%s__assetDescription__")
}

fn parse_asset_labels(value: &str) -> Vec<String> {
    value
        .split("__")
        .map(|l| l.to_owned())
        .filter(|l| !l.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::escape_unicode_null;
    use super::parse_asset_labels;

    #[test]
    fn should_escape_unicode_null() {
        assert!("asd\0".contains("\0"));
        assert_eq!(escape_unicode_null("asd\0"), "asd\\0");
    }

    #[test]
    fn should_filter_empty_labels() {
        assert_eq!(parse_asset_labels(""), [] as [&str; 0]);
        assert_eq!(parse_asset_labels("__"), [] as [&str; 0]);
        assert_eq!(parse_asset_labels("____"), [] as [&str; 0]);
        assert_eq!(parse_asset_labels("DEFO"), ["DEFO"]);
        assert_eq!(parse_asset_labels("__DEFO"), ["DEFO"]);
        assert_eq!(parse_asset_labels("DEFO__"), ["DEFO"]);
        assert_eq!(parse_asset_labels("__DEFO__"), ["DEFO"]);
        assert_eq!(parse_asset_labels("DEFO__GATEWAY"), ["DEFO", "GATEWAY"]);
        assert_eq!(parse_asset_labels("DEFO__GATEWAY__"), ["DEFO", "GATEWAY"]);
        assert_eq!(parse_asset_labels("__DEFO__GATEWAY"), ["DEFO", "GATEWAY"]);
        assert_eq!(parse_asset_labels("__DEFO__GATEWAY__"), ["DEFO", "GATEWAY"]);
        assert_eq!(parse_asset_labels("DEFO____GATEWAY"), ["DEFO", "GATEWAY"]);
    }
}
