use serde::{Deserialize, Deserializer};

use crate::models::{AssetLabel, VerificationStatus};

use super::DEFAULT_LIMIT;

#[derive(Clone, Debug, Deserialize)]
pub struct SearchRequest {
    pub ids: Option<Vec<String>>,
    pub ticker: Option<String>,
    pub search: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub smart: Option<bool>,
    pub verified_status: Option<Vec<VerificationStatus>>,
    #[serde(rename = "label__in")]
    pub asset_label_in: Option<Vec<AssetLabel>>,
    pub limit: Option<u32>,
    pub after: Option<String>,
}

impl From<SearchRequest> for crate::services::assets::SearchRequest {
    fn from(sr: SearchRequest) -> Self {
        Self {
            ids: sr.ids,
            ticker: sr.ticker,
            search: sr.search,
            smart: sr.smart,
            verification_status_in: sr.verified_status,
            asset_label_in: sr.asset_label_in,
            limit: sr.limit.unwrap_or(DEFAULT_LIMIT),
            after: sr.after,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MgetRequest {
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RequestOptions {
    pub format: Option<ResponseFormat>,
    pub include_metadata: Option<bool>,
    #[serde(rename = "height__gte")]
    pub height_gte: Option<i32>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormat {
    Full,
    Brief,
}

fn deserialize_optional_bool_from_string<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer) {
        Ok(s) => {
            if s.to_lowercase() == "true" {
                Ok(Some(true))
            } else if s.to_lowercase() == "false" {
                Ok(Some(false))
            } else {
                Err(serde::de::Error::custom(format!(
                    "Got unexpected string while deserializing bool from string: {}",
                    s
                )))
            }
        }
        Err(e) => {
            println!("Error occurred while deserializing optinal bool from string");
            Err(e)
        }
    }
}

/// Escapes querystring field replacing sequence params with `<field>[]`
///
/// Backward compatibility door for clients,
/// that requests service with params like `ids=1&ids=2`, `ids%5B%5D=1&ids%5B%5D=2` etc.
pub fn escape_querystring_field<'de>(qs: &'de str, field: &str) -> String {
    let to = format!("{}[]=", field);
    qs.replace(&format!("{}=", field), &to)
        .replace(&format!("{}%5B%5D=", field), &to)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::deserialize_optional_bool_from_string;

    #[derive(Deserialize, Debug, Clone)]
    pub struct Element {
        #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
        value: Option<bool>,
    }

    #[test]
    fn should_deserialize_optional_bool_from_string() {
        let e: Element = serde_qs::from_str(r#""#).unwrap();
        assert_eq!(e.value, None);

        let e: Element = serde_qs::from_str(r#"value=true"#).unwrap();
        assert_eq!(e.value, Some(true));

        let e: Element = serde_qs::from_str(r#"value=True"#).unwrap();
        assert_eq!(e.value, Some(true));

        let e: Element = serde_qs::from_str(r#"value=false"#).unwrap();
        assert_eq!(e.value, Some(false));

        let r: Result<Option<bool>, _> = serde_qs::from_str(r#"value=asd"#);
        assert!(matches!(r, Err(_)));
    }
}
