table! {
    asset_metadatas (id, name) {
        id -> Text,
        name -> Text,
        ticker -> Nullable<Text>,
        height -> Int4,
    }
}

table! {
    asset_wx_labels (asset_id, label) {
        asset_id -> Text,
        label -> Text,
    }
}

table! {
    asset_labels (superseded_by, asset_id) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        asset_id -> Text,
        labels -> Array<Text>,
    }
}

table! {
    asset_labels_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    asset_tickers (superseded_by, asset_id) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        asset_id -> Text,
        ticker -> Text,
    }
}

table! {
    asset_tickers_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    asset_names (superseded_by, asset_id) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        asset_id -> Text,
        asset_name -> Text,
    }
}

table! {
    asset_names_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    asset_descriptions (superseded_by, asset_id) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        asset_id -> Text,
        asset_description -> Text,
    }
}

table! {
    asset_descriptions_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    assets (superseded_by, id) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        id -> Text,
        name -> Text,
        description -> Text,
        time_stamp -> Timestamptz,
        issuer -> Text,
        precision -> Int4,
        smart -> Bool,
        nft -> Bool,
        quantity -> Int8,
        reissuable -> Bool,
        min_sponsored_fee -> Nullable<Int8>,
    }
}

table! {
    assets_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    blocks_microblocks (id) {
        uid -> Int8,
        id -> Text,
        height -> Int4,
        time_stamp -> Nullable<Int8>,
    }
}

table! {
    data_entries (superseded_by, address, key) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        address -> Text,
        key -> Text,
        data_type -> Nullable<crate::db::enums::DataEntryValueTypeMapping>,
        int_val -> Nullable<Int8>,
        bool_val -> Nullable<Bool>,
        bin_val -> Nullable<Bytea>,
        str_val -> Nullable<Text>,
        related_asset_id -> Nullable<Text>,
    }
}

table! {
    data_entries_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    issuer_balances (superseded_by, address) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        address -> Text,
        regular_balance -> Int8,
    }
}

table! {
    issuer_balances_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    out_leasings (superseded_by, address) {
        uid -> Int8,
        superseded_by -> Int8,
        block_uid -> Int8,
        address -> Text,
        amount -> Int8,
    }
}

table! {
    out_leasings_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

allow_tables_to_appear_in_same_query!(
    asset_metadatas,
    asset_wx_labels,
    assets,
    blocks_microblocks,
    data_entries,
    issuer_balances,
    out_leasings,
    asset_names,
    asset_descriptions,
);
