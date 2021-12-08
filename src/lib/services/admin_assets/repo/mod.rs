pub mod pg;

use anyhow::Result;

use crate::db::enums::{AssetWxLabelValueType, VerificationStatusValueType};

pub trait Repo {
    fn set_verification_status(
        &self,
        id: &str,
        verification_status: &VerificationStatusValueType,
    ) -> Result<bool>;

    fn update_ticker(&self, id: &str, ticker: Option<&str>) -> Result<bool>;

    fn add_label(&self, id: &str, label: &AssetWxLabelValueType) -> Result<bool>;

    fn delete_label(&self, id: &str, label: &AssetWxLabelValueType) -> Result<bool>;
}
