use std::fmt::Debug;

use crate::util::encode;
use crate::util::Decode;
use crate::util::Encode;

pub(crate) fn assert_roundtrip<E: Encode + Decode + Debug + Eq>(e: &E) -> anyhow::Result<()> {
    let encoded = encode(e);
    let decoded = E::decode(&encoded)?;
    assert_eq!(e, &decoded);
    Ok(())
}
