use itertools::Itertools;
use serde_qs::Config;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use validator::Validate;
use warp::{Filter, Rejection};
use wavesexchange_log::{debug, error, info};
use wavesexchange_warp::error::{
    error_handler_with_serde_qs, handler, internal, timeout, validation,
};
use wavesexchange_warp::{
    endpoints::{livez, readyz, startz},
    log::access,
};

use super::dtos::{escape_querystring_field, MgetRequest, RequestOptions, SearchRequest};
use super::models::{Asset, AssetInfo, List};
use super::{DEFAULT_FORMAT, DEFAULT_INCLUDE_METADATA, DEFAULT_LIMIT, ERROR_CODES_PREFIX};
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

    let error_handler = handler(ERROR_CODES_PREFIX, |err| match err {
        error::Error::ValidationError(field, error_details) => {
            let mut error_details = error_details.to_owned();
            if let Some(details) = error_details.as_mut() {
                details.insert("parameter".to_owned(), field.to_owned());
            }
            validation::invalid_parameter(ERROR_CODES_PREFIX, error_details)
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
        // parse SearchRequest
        .and(
            warp::query::raw()
                .or_else(|_rej| futures::future::ok::<(String,), Infallible>(("".to_owned(),)))
                .and_then(|qs: String| async move {
                    let cfg = create_serde_qs_config();
                    let qs = escape_querystring_field(&qs, "ids");
                    let qs = escape_querystring_field(&qs, "label__in");
                    let qs = escape_querystring_field(&qs, "verified_status");
                    parse_querystring(&cfg, qs.as_str())
                })
                .and_then(|value| async move { validate(value).map_err(warp::reject::custom) }),
        )
        // parse RequestOptions
        .and(
            warp::query::raw()
                .or_else(|_rej| futures::future::ok::<(String,), Infallible>(("".to_owned(),)))
                .and_then(|qs: String| async move {
                    let cfg = create_serde_qs_config();
                    let qs = escape_querystring_field(&qs, "ids");
                    let qs = escape_querystring_field(&qs, "label__in");
                    let qs = escape_querystring_field(&qs, "verified_status");
                    parse_querystring(&cfg, qs.as_str())
                })
                .and_then(|value| async move { validate(value).map_err(warp::reject::custom) }),
        )
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

    let routes = livez()
        .or(readyz())
        .or(startz())
        .or(assets_get_handler)
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
    debug!("assets_get_controller"; "req" => format!("{:?}", req), "opts" => format!("{:?}", opts));

    let limit = req.limit.unwrap_or(DEFAULT_LIMIT);
    let include_metadata = opts.include_metadata.unwrap_or(DEFAULT_INCLUDE_METADATA);
    let format = opts.format.unwrap_or(DEFAULT_FORMAT);

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

    let assets = assets_service.mget(&asset_ids, &mget_options).await?;

    let has_images = if include_metadata {
        images_service.has_images(&asset_ids).await?
    } else {
        vec![false; asset_ids.len()]
    };

    let assets = assets
        .into_iter()
        .zip(has_images)
        .map(|(o, has_image)| Asset::new(o, has_image, include_metadata, &format))
        .collect_vec();

    let last_cursor = if has_next_page {
        assets.last().and_then(|a| {
            a.data.as_ref().map(|ai| match ai {
                AssetInfo::Full(ai) => ai.id.clone(),
                AssetInfo::Brief(ai) => ai.id.clone(),
            })
        })
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

    let include_metadata = opts.include_metadata.unwrap_or(DEFAULT_INCLUDE_METADATA);
    let format = opts.format.unwrap_or(DEFAULT_FORMAT);

    let asset_ids = req.ids.iter().map(AsRef::as_ref).collect_vec();

    let mget_options = match opts.height_gte {
        Some(height) => MgetOptions::with_height(height),
        _ => MgetOptions::default(),
    };

    let assets = assets_service.mget(&asset_ids, &mget_options).await?;

    let has_images = if include_metadata {
        images_service.has_images(&asset_ids).await?
    } else {
        vec![false; asset_ids.len()]
    };

    let list = List {
        data: assets
            .into_iter()
            .zip(has_images)
            .map(|(o, has_image)| Asset::new(o, has_image, include_metadata, &format))
            .collect_vec(),
        cursor: None,
    };

    Ok(list)
}

fn create_serde_qs_config() -> serde_qs::Config {
    serde_qs::Config::new(5, false)
}

/// Parses querystring into T using serde_qs_config
pub fn parse_querystring<'de, T>(serde_qs_config: &Config, qs: &'de str) -> Result<T, Rejection>
where
    T: serde::de::Deserialize<'de>,
{
    serde_qs_config
        .deserialize_str::<T>(&qs)
        .map_err(|e| warp::reject::custom(e))
}

fn validate<T>(value: T) -> Result<T, error::Error>
where
    T: Validate,
{
    value.validate().map_err(|errs| {
        let errors = errs.errors();
        if errors.len() > 0 {
            // todo: handle not only the 1st error
            let (field_name, error_details) = errors.iter().next().unwrap();
            match error_details {
                validator::ValidationErrorsKind::Field(error_details) => {
                    // todo: handle not only the 1st error
                    let details = error_details.iter().next().map(|e| {
                        vec![("reason".to_owned(), e.code.to_string())]
                            .into_iter()
                            .collect::<HashMap<String, String>>()
                    });
                    error::Error::ValidationError(field_name.to_string(), details)
                }
                validator::ValidationErrorsKind::List(_)
                | validator::ValidationErrorsKind::Struct(_) => {
                    error::Error::ValidationError(field_name.to_string(), None)
                }
            }
        } else {
            error::Error::ValidationError(errs.to_string(), None)
        }
    })?;

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::super::{
        dtos::SearchRequest,
        server::{create_serde_qs_config, parse_querystring},
    };

    #[test]
    fn should_parse_querystring() {
        let cfg = create_serde_qs_config();
        let ids = vec!["1".to_owned(), "2".to_owned()];

        let res = parse_querystring::<SearchRequest>(&cfg, r"ids=1&ids=2");

        assert!(matches!(res, Ok(_)));
        assert!(matches!(res.as_ref().unwrap().ids, Some(_)));
        assert_eq!(res.unwrap().ids.unwrap(), ids);

        let res = parse_querystring::<SearchRequest>(&cfg, r"ids[]=1&ids[]=2");

        assert!(matches!(res, Ok(_)));
        assert!(matches!(res.as_ref().unwrap().ids, Some(_)));
        assert_eq!(res.unwrap().ids.unwrap(), ids);

        let res = parse_querystring::<SearchRequest>(&cfg, r"ids%5B%5D=1&ids%5B%5D=2");

        assert!(matches!(res, Ok(_)));
        assert!(matches!(res.as_ref().unwrap().ids, Some(_)));
        assert_eq!(res.unwrap().ids.unwrap(), ids);

        let res = parse_querystring::<SearchRequest>(&cfg, r"search=asd");

        assert!(matches!(res, Ok(_)));
        assert!(matches!(res.unwrap().ids, None));
    }
}
