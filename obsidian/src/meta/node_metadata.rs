use crate::meta::MetaValue;

pub(crate) struct NodeMetadata();

impl MetaValue for NodeMetadata {
    type PB = ();
}

impl TryFrom<()> for NodeMetadata {
    type Error = anyhow::Error;

    fn try_from(_: ()) -> Result<Self, Self::Error> {
        Ok(Self())
    }
}

impl From<NodeMetadata> for () {
    fn from(_: NodeMetadata) -> Self {
        ()
    }
}
