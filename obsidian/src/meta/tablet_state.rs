use std::fmt::Debug;

use anyhow::anyhow;

use crate::pb;

/// TabletState facilitates transfers of key ranges between tablets.
///
/// State properties shown as `[crw]` for complete, readable, writable. Complete means that the
/// tablet holds all of the data for a key range. As such, it is impossible to have a `[c**]`
/// tablet concurrently with a separate tablet in `[**w]`, because as soon as the `w` tablet
/// accepts a write the `c` tablet would no longer be complete.
///
/// The read and write state properties are enforced by the state machine in DataTablet. Complete is
/// more subtle, it's guaranteed by the mechanics of transfer.
///
/// In a range transfer, the source tablet(s) start at Active and the destination(s) start at
/// Hydrating. The goal is to get the source(s) to Defunct and the destination(s) to Active.
///
/// ```text
///                                      ┌─────────────────┐                                        
///       new transfer destination╶─────>│ Hydrating [___] ├──────────────────┐                     
///                                      └────────┬────────┘                  │                     
///                                               │                           │                     
///                                               v                           v                     
///                                       ┌────────────────┐             ┌─────────┐                
///                                       │ Frozen   [cr_] ├────────────>│ Defunct │                
///                                       └────┬───────────┘             └─────────┘                
///                                            │     ^                                              
///                                            v     │                                              
///                                       ┌──────────┴─────┐                                        
///           new colo group╶────────────>│ Active   [crw] │                                        
///                                       └────────────────┘                                        
/// ```
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum TabletState {
    /// The tablet is abandoned, either because the key range it held has been transferred to other
    /// tablet(s) or because the transfer it was created for was aborted.
    Defunct,
    /// The tablet is copying data from another tablet. It cannot serve any reads or writes.
    Hydrating,
    /// The tablet holds the authoritative copy of the data and can accept reads and writes.
    Active,
    /// The tablet holds a full copy of the data, but can only accept reads and not writes.
    Frozen,
}

impl From<TabletState> for pb::internal::TabletState {
    fn from(value: TabletState) -> Self {
        match value {
            TabletState::Defunct => pb::internal::TabletState::Defunct,
            TabletState::Hydrating => pb::internal::TabletState::Hydrating,
            TabletState::Active => pb::internal::TabletState::Active,
            TabletState::Frozen => pb::internal::TabletState::Frozen,
        }
    }
}

impl TryFrom<pb::internal::TabletState> for TabletState {
    type Error = anyhow::Error;

    fn try_from(value: pb::internal::TabletState) -> Result<Self, Self::Error> {
        Ok(match value {
            pb::internal::TabletState::Defunct => TabletState::Defunct,
            pb::internal::TabletState::Hydrating => TabletState::Hydrating,
            pb::internal::TabletState::Active => TabletState::Active,
            pb::internal::TabletState::Frozen => TabletState::Frozen,
            _ => return Err(anyhow!("unrecognized TabletState {:?}", value)),
        })
    }
}
