use itertools::Itertools;
use std::sync::Arc;
use warp::{Filter, Rejection};
use wavesexchange_log::{debug, error, info};
use wavesexchange_warp::error::{
    error_handler_with_serde_qs, handler, internal, timeout, validation,
};
use wavesexchange_warp::log::access;

use super::dtos::{MgetRequest, RequestOptions, SearchRequest};
use super::models::{Asset, List};
use super::{DEFAULT_LIMIT, ERROR_CODES_PREFIX};
use crate::error;
use crate::services;
use crate::services::assets::MgetOptions;

pub async fn start(
    port: u16,
    assets_service: impl services::assets::Service + Send + Sync + 'static,
    images_service: impl services::images::Service + Send + Sync + 'static,
) {
    let with_assets_service = {
        let assets_service = Arc::new(assets_service);
        warp::any().map(move || assets_service.clone())
    };

    let with_images_service = {
        let images_service = Arc::new(images_service);
        warp::any().map(move || images_service.clone())
    };

    let create_serde_qs_config = || serde_qs::Config::new(5, false);

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
            error!("{:?}", err);
            timeout(ERROR_CODES_PREFIX)
        }
        _ => {
            error!("{:?}", err);
            internal(ERROR_CODES_PREFIX)
        }
    });

    let assets_get_handler = warp::path!("assets")
        .and(warp::get())
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(serde_qs::warp::query::<SearchRequest>(
            create_serde_qs_config(),
        ))
        .and(serde_qs::warp::query::<RequestOptions>(
            create_serde_qs_config(),
        ))
        .and_then(assets_get_controller)
        .map(|res| warp::reply::json(&res));

    let assets_post_handler = warp::path!("assets")
        .and(warp::post())
        .and(with_assets_service.clone())
        .and(with_images_service.clone())
        .and(warp::body::json::<MgetRequest>())
        .and(serde_qs::warp::query::<RequestOptions>(
            create_serde_qs_config(),
        ))
        .and_then(assets_post_controller)
        .map(|res| warp::reply::json(&res));

    let log = warp::log::custom(access);

    info!("Starting API server at 0.0.0.0:{}", port);

    let routes = assets_get_handler
        .or(assets_post_handler)
        .recover(move |rej| {
            error!("{:?}", rej);
            error_handler_with_serde_qs(ERROR_CODES_PREFIX, error_handler.clone())(rej)
        })
        .with(log);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

async fn assets_get_controller(
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    req: SearchRequest,
    opts: RequestOptions,
) -> Result<List<Asset>, Rejection> {
    debug!("assets_get_controller"; "req" => format!("{:?}", req));

    let limit = req.limit.unwrap_or(DEFAULT_LIMIT);
    let include_metadata = opts.include_metadata.unwrap_or(true);

    let asset_ids: Vec<String> = if let Some(ids) = req.ids {
        ids
    } else {
        let req = services::assets::SearchRequest::from(req).with_limit(limit + 1);
        assets_service.search(&req)?
    };

    let has_next_page = if asset_ids.len() as u32 > limit {
        true
    } else {
        false
    };

    let asset_ids = asset_ids
        .iter()
        .take(limit as usize)
        .map(AsRef::as_ref)
        .collect_vec();

    let mget_options = match opts.height_gte {
        Some(height) => MgetOptions::with_height(height),
        _ => MgetOptions::default(),
    };

    let assets = assets_service.mget(&asset_ids, &mget_options)?;

    let has_images = images_service.has_images(&asset_ids).await?;

    let assets = assets
        .into_iter()
        .zip(has_images)
        .map(|(o, has_image)| Asset::from((o, has_image, include_metadata)))
        .collect_vec();

    let last_cursor = if has_next_page {
        assets
            .last()
            .and_then(|a| a.data.as_ref().map(|ai| ai.id.clone()))
    } else {
        None
    };

    let list = List {
        data: assets,
        cursor: last_cursor,
    };

    Ok(list)
}

async fn assets_post_controller(
    assets_service: Arc<impl services::assets::Service>,
    images_service: Arc<impl services::images::Service>,
    req: MgetRequest,
    opts: RequestOptions,
) -> Result<List<Asset>, Rejection> {
    debug!("assets_post_controller");
    let include_metadata = opts.include_metadata.unwrap_or(true);

    let asset_ids = req.ids.iter().map(AsRef::as_ref).collect_vec();

    let mget_options = match opts.height_gte {
        Some(height) => MgetOptions::with_height(height),
        _ => MgetOptions::default(),
    };

    let assets = assets_service.mget(&asset_ids, &mget_options)?;

    let has_images = images_service.has_images(&asset_ids).await?;

    let list = List {
        data: assets
            .into_iter()
            .zip(has_images)
            .map(|(o, has_image)| Asset::from((o, has_image, include_metadata)))
            .collect_vec(),
        cursor: None,
    };

    Ok(list)
}
