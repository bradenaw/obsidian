use prost::Message;

pub(crate) trait MetaValue:
    Into<Self::PB> + TryFrom<Self::PB, Error = anyhow::Error>
{
    type PB: prost::Message + Default;

    fn encode_to_vec(self) -> Vec<u8> {
        Into::<Self::PB>::into(self).encode_to_vec()
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(Self::try_from(Self::PB::decode(b)?)?)
    }
}
