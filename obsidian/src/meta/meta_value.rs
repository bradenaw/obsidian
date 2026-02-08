use prost::Message;

pub(crate) trait MetaValue {
    type PB: prost::Message + Default;

    fn encode_to_vec(self) -> Vec<u8>
    where
        Self: Into<Self::PB> + Sized,
    {
        Into::<Self::PB>::into(self).encode_to_vec()
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self>
    where
        Self: TryFrom<Self::PB, Error = anyhow::Error> + Sized,
    {
        Ok(Self::try_from(Self::PB::decode(b)?)?)
    }
}
