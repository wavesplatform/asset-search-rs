use crate::schema::blocks_microblocks;

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[diesel(table_name = blocks_microblocks)]
pub struct BlockMicroblock {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
}
