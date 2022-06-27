use diesel_derive_enum::DbEnum;

#[derive(DbEnum, Clone, Debug)]
pub enum DataEntryValueType {
    Bin,
    Bool,
    Int,
    Str,
}
