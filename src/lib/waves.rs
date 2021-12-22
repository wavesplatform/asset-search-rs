use bytes::{BufMut, BytesMut};
use std::convert::TryInto;

pub fn keccak256(message: &[u8]) -> [u8; 32] {
    use sha3::{Digest, Keccak256};

    let mut hasher = Keccak256::new();

    hasher.update(message);

    hasher.finalize().into()
}

pub fn blake2b256(message: &[u8]) -> [u8; 32] {
    use blake2::digest::Update;
    use blake2::digest::VariableOutput;
    use blake2::VarBlake2b;

    let mut hasher = VarBlake2b::new(32).unwrap();

    hasher.update(message);

    let mut arr = [0u8; 32];

    hasher.finalize_variable(|res| arr = res.try_into().unwrap());

    arr
}

pub struct Address(String);
pub struct RawPublicKey(Vec<u8>);
pub struct RawAddress(Vec<u8>);

impl From<(RawPublicKey, u8)> for Address {
    fn from(data: (RawPublicKey, u8)) -> Self {
        let (RawPublicKey(pk), chain_id) = data;

        let pkh = keccak256(&blake2b256(&pk));

        let mut addr = BytesMut::with_capacity(26); // VERSION + CHAIN_ID + PKH + checksum

        addr.put_u8(1); // address version is always 1
        addr.put_u8(chain_id);
        addr.put_slice(&pkh[..20]);

        let chks = &keccak256(&blake2b256(&addr[..22]))[..4];

        addr.put_slice(chks);

        Address(bs58::encode(addr).into_string())
    }
}

impl From<(&[u8], u8)> for Address {
    fn from(data: (&[u8], u8)) -> Self {
        let (pk, chain_id) = data;

        let pkh = keccak256(&blake2b256(pk));

        let mut addr = BytesMut::with_capacity(26); // VERSION + CHAIN_ID + PKH + checksum

        addr.put_u8(1); // address version is always 1
        addr.put_u8(chain_id);
        addr.put_slice(&pkh[..20]);

        let chks = &keccak256(&blake2b256(&addr[..22]))[..4];

        addr.put_slice(chks);

        Address(bs58::encode(addr).into_string())
    }
}

impl From<(RawAddress, u8)> for Address {
    fn from(data: (RawAddress, u8)) -> Self {
        let (RawAddress(address), chain_id) = data;

        let mut addr = BytesMut::with_capacity(26);

        addr.put_u8(1);
        addr.put_u8(chain_id);
        addr.put_slice(&address[..]);

        let chks = &keccak256(&blake2b256(&addr[..22]))[..4];

        addr.put_slice(chks);

        Address(bs58::encode(addr).into_string())
    }
}

impl From<Address> for String {
    fn from(v: Address) -> Self {
        v.0
    }
}

pub const WAVES_ID: &str = "WAVES";
pub const WAVES_NAME: &str = "Waves";
pub const WAVES_PRECISION: i32 = 8;

pub fn get_asset_id<I: AsRef<[u8]>>(input: I) -> String {
    if input.as_ref().is_empty() {
        WAVES_ID.to_owned()
    } else {
        bs58::encode(input).into_string()
    }
}

pub fn is_waves_asset_id<I: AsRef<[u8]>>(input: I) -> bool {
    get_asset_id(input) == WAVES_ID
}

pub fn is_nft_asset(quantity: i64, precision: i32, reissuable: bool) -> bool {
    quantity == 1 && precision == 0 && !reissuable
}

#[cfg(test)]
mod tests {
    use super::is_nft_asset;

    #[test]
    fn should_recognize_nft_asset() {
        // quantity, precision, reissuable, expected
        let test_cases = [
            (0, 0, false, false),
            (0, 0, true, false),
            (0, 1, false, false),
            (0, 1, true, false),
            (1, 0, false, true),
            (1, 0, true, false),
            (1, 1, false, false),
            (1, 1, true, false),
            (2, 0, false, false),
            (2, 0, true, false),
            (2, 1, false, false),
            (2, 1, true, false),
        ];

        test_cases.iter().for_each(|tc| {
            let quantity = tc.0;
            let precision = tc.1;
            let reissuable = tc.2;

            assert_eq!(tc.3, is_nft_asset(quantity, precision, reissuable));
        });
    }
}
