use serde::{Deserialize, Deserializer};
use validator::{Validate, ValidationError};

use super::DEFAULT_LIMIT;
use crate::waves::is_valid_base58;

#[derive(Clone, Debug, Deserialize, Validate)]
pub struct SearchRequest {
    pub ids: Option<Vec<String>>,
    pub ticker: Option<String>,
    pub search: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub smart: Option<bool>,
    #[serde(rename = "label__in")]
    pub asset_label_in: Option<Vec<String>>,
    #[serde(rename = "issuer__in")]
    #[validate(custom = "validate_issuer_in")]
    pub issuer_in: Option<Vec<String>>,
    #[validate(range(max = 100))]
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
            asset_label_in: sr.asset_label_in,
            limit: sr.limit.unwrap_or(DEFAULT_LIMIT),
            issuer_in: sr.issuer_in,
            after: sr.after,
        }
    }
}

fn validate_issuer_in(issuers: &Vec<String>) -> Result<(), ValidationError> {
    issuers.iter().fold(Ok(()), |_, addr| {
        if !is_valid_base58(addr) {
            Err(ValidationError::new("Got invalid base58 string"))
        } else {
            Ok(())
        }
    })
}

#[derive(Clone, Debug, Deserialize)]
pub struct MgetRequest {
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Validate)]
pub struct RequestOptions {
    pub format: Option<ResponseFormat>,
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
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
