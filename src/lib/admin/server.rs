use futures::TryFutureExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use warp::{reject, Filter, Rejection};
use wavesexchange_log::{debug, error, info};
use wavesexchange_warp::error::{
    authorization, error_handler_with_serde_qs, handler, internal, timeout, validation,
};
use wavesexchange_warp::log::access;

use super::InvalidateCacheQueryParams;
use crate::api::{dtos::ResponseFormat, models::Asset};
use crate::cache::{self, AssetBlockchainData, AssetUserDefinedData, InvalidateCacheMode};
use crate::error;
use crate::models::{AssetLabel, VerificationStatus};
use crate::services;
use crate::services::assets::GetOptions;

const ERROR_CODES_PREFIX: u16 = 95;
const API_KEY_HEADER_NAME: &str = "X-Api-Key";
const DEFAULT_INCLUDE_METADATA: bool = true;
const DEFAULT_FORMAT: ResponseFormat = ResponseFormat::Full;

pub async fn start(
    port: u16,
    assets_service: impl services::assets::Service + Send + Sync + 'static,
    images_service: impl services::images::Service + Send + Sync + 'static,
    admin_assets_service: impl services::admin_assets::Service + Send + Sync + 'static,
    assets_blockchain_data_redis_cache: impl cache::AsyncWriteCache<AssetBlockchainData>
        + Send
        + Sync
        + 'static,
    assets_user_defined_data_redis_cache: impl cache::AsyncWriteCache<AssetUserDefinedData>
        + Send
        + Sync
        + 'static,
    api_key: String,
) {
    let with_assets_service = {
        let assets_service = Arc::new(assets_service);
        warp::any().map(move || assets_service.clone())
    };

    let with_images_service = {
        let images_service = Arc::new(images_service);
        warp::any().map(move || images_service.clone())
    };

    let with_admin_assets_service = {
        let admin_assets_service = Arc::new(admin_assets_service);
        warp::any().map(move || admin_assets_service.clone())
    };

    let with_assets_blockchain_data_redis_cache = {
        let assets_blockchain_data_redis_cache = Arc::new(assets_blockchain_data_redis_cache);
        warp::any().map(move || assets_blockchain_data_redis_cache.clone())
    };

    let with_assets_user_defined_data_redis_cache = {
        let assets_user_defined_data_redis_cache = Arc::new(assets_user_defined_data_redis_cache);
        warp::any().map(move || assets_user_defined_data_redis_cache.clone())
    };

    let with_api_key = warp::any().map(move || api_key.to_owned());

    let error_handler = handler(ERROR_CODES_PREFIX, |err| match err {
        error::Error::ValidationError(_error_message, error_details) => {
            validation::invalid_parameter(
                ERROR_CODES_PREFIX,
                error_details.to_owned().map(|details| details.into()),
            )
        }
        error::Error::DbDieselError(error_message)
            if error_message.to_string() == "canceling statement due to statement timeout" =>
        {
            timeout(ERROR_CODES_PREFIX)
        }
        error::Error::Unauthorized(_error_message) => authorization(ERROR_CODES_PREFIX),
        error::Error::InvalidVariant(error_message) => {
            let details = vec![("reason", error_message)]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect::<HashMap<String, String>>();
            validation::invalid_parameter(ERROR_CODES_PREFIX, Some(details))
        }
        _ => internal(ERROR_CODES_PREFIX),
    });

    let asset_verification_status_handler =
        warp::path!("admin" / "asset" / String / "verification" / String)
            .and(warp::post())
            .and(with_api_key.clone())
            .and(warp::header::<String>(API_KEY_HEADER_NAME))
            .and(with_assets_service.clone())
            .and(with_images_service.clone())
            .and(with_admin_assets_service.clone())
            .and_then(
                move |asset_id: String,
                      verification_status: String,
                      expected_api_key: String,
                      provided_api_key: String,
                      assets_service,
                      images_service,
                      admin_assets_service| async move {
                    api_key_validation(&expected_api_key, &provided_api_key)
                        .and_then(|_| {
                            asset_verification_status_controller(
                                asset_id,
                                verification_status,
                                assets_service,
                                images_service,
                                admin_assets_service,
                            )
                        })
                        .await
                },
            )
            .map(|res| warp::reply::json(&res));

    let asset_set_ticker_handler = warp::path!("admin" / "asset" / String / "ticker" / String)
        .and(warp::post())
        .and(with_api_key.clone())
        .and(warp::header::<String>(API_KEY_HEADER_NAME))
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(with_admin_assets_service.clone())
        .and_then(
            |asset_id: String,
             ticker: String,
             expected_api_key: String,
             provided_api_key: String,
             assets_service,
             images_service,
             admin_assets_service| async move {
                api_key_validation(&expected_api_key, &provided_api_key)
                    .and_then(|_| {
                        asset_set_ticker_controller(
                            asset_id,
                            ticker,
                            assets_service,
                            images_service,
                            admin_assets_service,
                        )
                    })
                    .await
            },
        )
        .map(|res| warp::reply::json(&res));

    let asset_delete_ticker_handler = warp::path!("admin" / "asset" / String / "ticker")
        .and(warp::delete())
        .and(with_api_key.clone())
        .and(warp::header::<String>(API_KEY_HEADER_NAME))
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(with_admin_assets_service.clone())
        .and_then(
            |asset_id: String,
             expected_api_key: String,
             provided_api_key: String,
             assets_service,
             images_service,
             admin_assets_service| async move {
                api_key_validation(&expected_api_key, &provided_api_key)
                    .and_then(|_| {
                        asset_delete_ticker_controller(
                            asset_id,
                            assets_service,
                            images_service,
                            admin_assets_service,
                        )
                    })
                    .await
            },
        )
        .map(|res| warp::reply::json(&res));

    let asset_add_label_handler = warp::post()
        .and(warp::path!("admin" / "asset" / String / "labels" / String))
        .and(with_api_key.clone())
        .and(warp::header::<String>(API_KEY_HEADER_NAME))
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(with_admin_assets_service.clone())
        .and_then(
            |asset_id: String,
             label: String,
             expected_api_key: String,
             provided_api_key: String,
             assets_service,
             images_service,
             admin_assets_service| async move {
                api_key_validation(&expected_api_key, &provided_api_key)
                    .and_then(|_| {
                        asset_add_label_controller(
                            asset_id,
                            label,
                            assets_service,
                            images_service,
                            admin_assets_service,
                        )
                    })
                    .await
            },
        )
        .map(|res| warp::reply::json(&res));

    let asset_delete_label_handler = warp::path!("admin" / "asset" / String / "labels" / String)
        .and(warp::delete())
        .and(with_api_key.clone())
        .and(warp::header::<String>(API_KEY_HEADER_NAME))
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(with_admin_assets_service.clone())
        .and_then(
            |asset_id: String,
             label: String,
             expected_api_key: String,
             provided_api_key: String,
             assets_service,
             images_service,
             admin_assets_service| async move {
                api_key_validation(&expected_api_key, &provided_api_key)
                    .and_then(|_| {
                        asset_delete_label_controller(
                            asset_id,
                            label,
                            assets_service,
                            images_service,
                            admin_assets_service,
                        )
                    })
                    .await
            },
        )
        .map(|res| warp::reply::json(&res));

    let cache_invalidate_handler = warp::post()
        .and(warp::path!("admin" / "cache" / "invalidate"))
        .and(warp::query::<InvalidateCacheQueryParams>())
        .and(with_api_key.clone())
        .and(warp::header::<String>(API_KEY_HEADER_NAME))
        .and(with_assets_service.clone())
        .and(with_assets_blockchain_data_redis_cache.clone())
        .and(with_assets_user_defined_data_redis_cache.clone())
        .and_then(
            |query: InvalidateCacheQueryParams,
             expected_api_key: String,
             provided_api_key: String,
             assets_service,
             assets_blockchain_data_redis_cache,
             assets_user_defined_data_redis_cache| async move {
                api_key_validation(&expected_api_key, &provided_api_key)
                    .and_then(|_| {
                        cache_invalidate_controller(
                            &query.mode,
                            assets_service,
                            assets_blockchain_data_redis_cache,
                            assets_user_defined_data_redis_cache,
                        )
                    })
                    .await
            },
        )
        .map(|res| warp::reply::json(&res));

    let log = warp::log::custom(access);

    info!("Starting API server at 0.0.0.0:{}", port);

    let routes = asset_verification_status_handler
        .or(asset_set_ticker_handler)
        .or(asset_delete_ticker_handler)
        .or(asset_add_label_handler)
        .or(asset_delete_label_handler)
        .or(cache_invalidate_handler)
        .recover(move |rej| {
            error!("rej: {:?}", rej);
            error_handler_with_serde_qs(ERROR_CODES_PREFIX, error_handler.clone())(rej)
        })
        .with(log);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

async fn asset_verification_status_controller(
    asset_id: String,
    verification_status: String,
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    admin_assets_service: Arc<impl services::admin_assets::Service>,
) -> Result<Asset, Rejection> {
    let verification_status = VerificationStatus::try_from(verification_status)?;
    debug!("asset_verification_status_controller"; "asset_id" => &asset_id, "verification_status" => format!("{}", verification_status));

    admin_assets_service
        .update_verification_status(&asset_id, &verification_status)
        .await?;

    let maybe_asset_info = assets_service
        .get(&asset_id, &GetOptions::default())
        .await?;
    let has_image = images_service.has_image(&asset_id).await?;

    Ok(Asset::new(
        maybe_asset_info,
        has_image,
        DEFAULT_INCLUDE_METADATA,
        &DEFAULT_FORMAT,
    ))
}

async fn asset_set_ticker_controller(
    asset_id: String,
    ticker: String,
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    admin_assets_service: Arc<impl services::admin_assets::Service>,
) -> Result<Asset, Rejection> {
    debug!("asset_set_ticker_controller"; "asset_id" => &asset_id, "ticker" => &ticker);

    admin_assets_service
        .update_ticker(&asset_id, Some(&ticker))
        .await?;

    let maybe_asset_info = assets_service
        .get(&asset_id, &GetOptions::default())
        .await?;
    let has_image = images_service.has_image(&asset_id).await?;

    Ok(Asset::new(
        maybe_asset_info,
        has_image,
        DEFAULT_INCLUDE_METADATA,
        &DEFAULT_FORMAT,
    ))
}

async fn asset_delete_ticker_controller(
    asset_id: String,
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    admin_assets_service: Arc<impl services::admin_assets::Service>,
) -> Result<Asset, Rejection> {
    debug!("asset_delete_ticker_controller"; "asset_id" => &asset_id);

    admin_assets_service.update_ticker(&asset_id, None).await?;

    let maybe_asset_info = assets_service
        .get(&asset_id, &GetOptions::default())
        .await?;
    let has_image = images_service.has_image(&asset_id).await?;

    Ok(Asset::new(
        maybe_asset_info,
        has_image,
        DEFAULT_INCLUDE_METADATA,
        &DEFAULT_FORMAT,
    ))
}

async fn asset_add_label_controller(
    asset_id: String,
    label: String,
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    admin_assets_service: Arc<impl services::admin_assets::Service>,
) -> Result<Asset, Rejection> {
    debug!("asset_add_label_controller"; "asset_id" => &asset_id, "label" => &label);

    let label = AssetLabel::try_from(label.as_str())?;

    admin_assets_service.add_label(&asset_id, &label).await?;

    let maybe_asset_info = assets_service
        .get(&asset_id, &GetOptions::default())
        .await?;
    let has_image = images_service.has_image(&asset_id).await?;

    Ok(Asset::new(
        maybe_asset_info,
        has_image,
        DEFAULT_INCLUDE_METADATA,
        &DEFAULT_FORMAT,
    ))
}

async fn asset_delete_label_controller(
    asset_id: String,
    label: String,
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    admin_assets_service: Arc<impl services::admin_assets::Service>,
) -> Result<Asset, Rejection> {
    debug!("asset_delete_label_controller"; "asset_id" => &asset_id, "label" => &label);

    let label = AssetLabel::try_from(label.as_str())?;

    admin_assets_service.delete_label(&asset_id, &label).await?;

    let maybe_asset_info = assets_service
        .get(&asset_id, &GetOptions::default())
        .await?;
    let has_image = images_service.has_image(&asset_id).await?;

    Ok(Asset::new(
        maybe_asset_info,
        has_image,
        DEFAULT_INCLUDE_METADATA,
        &DEFAULT_FORMAT,
    ))
}

async fn cache_invalidate_controller<S, BDC, UDDC>(
    invalidate_cache_mode: &InvalidateCacheMode,
    assets_service: Arc<S>,
    assets_blockchain_data_redis_cache: Arc<BDC>,
    assets_user_defined_data_redis_cache: Arc<UDDC>,
) -> Result<(), Rejection>
where
    S: services::assets::Service,
    BDC: cache::AsyncWriteCache<AssetBlockchainData>,
    UDDC: cache::AsyncWriteCache<AssetUserDefinedData>,
{
    debug!("cache_invalidate_controller");

    crate::cache::invalidator::run(
        assets_service.clone(),
        assets_blockchain_data_redis_cache.clone(),
        assets_user_defined_data_redis_cache.clone(),
        invalidate_cache_mode,
    )
    .await
    .map_err(|e| error::Error::InvalidateCacheError(e.to_string()))?;

    Ok(())
}

async fn api_key_validation(expected: &str, provided: &str) -> Result<(), Rejection> {
    if expected == provided {
        Ok(())
    } else {
        Err(reject::custom(error::Error::Unauthorized(
            "Invalid API Key".to_owned(),
        )))
    }
}
