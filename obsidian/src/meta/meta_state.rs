/// Several of the metadata types have associated state machines. Transitions of those state
/// machines requires other nodes in the system to take some actions (e.g. transitioning a tablet
/// from Active to Frozen requires revoking write access), we represent those transitions inside
/// Meta with an additional in-between state, meaning the transition has started but the actual
/// acction needed to be taken may not have happened yet.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum MetaState<T> {
    Stable(T),
    Transitioning(T, T),
}

impl<T> MetaState<T> {
    pub fn current(&self) -> &T {
        match self {
            Self::Stable(curr) => curr,
            Self::Transitioning(curr, _) => curr,
        }
    }

    pub fn next(&self) -> Option<&T> {
        match self {
            Self::Stable(_) => None,
            Self::Transitioning(_, next) => Some(next),
        }
    }
}

impl<T> From<(T, Option<T>)> for MetaState<T> {
    fn from(value: (T, Option<T>)) -> Self {
        match value.1 {
            None => Self::Stable(value.0),
            Some(next) => Self::Transitioning(value.0, next),
        }
    }
}
