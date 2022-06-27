use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SearchRequest {
    pub ids: Option<Vec<String>>,
    pub ticker: Option<String>,
    pub search: Option<String>,
    pub smart: Option<bool>,
    pub asset_label_in: Option<Vec<String>>,
    pub issuer_in: Option<Vec<String>>,
    pub limit: u32,
    pub after: Option<String>,
}

impl SearchRequest {
    pub fn with_limit(&self, limit: u32) -> Self {
        let mut req = self.clone();
        req.limit = limit;
        req
    }

    pub fn with_after(&self, after: String) -> Self {
        let mut req = self.clone();
        req.after = Some(after);
        req
    }
}
