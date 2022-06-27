pub mod pg;

use anyhow::Result;

pub trait Repo {
    fn add_label(&self, id: &str, label: &str) -> Result<bool>;

    fn delete_label(&self, id: &str, label: &str) -> Result<bool>;
}
