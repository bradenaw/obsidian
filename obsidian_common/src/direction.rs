use std::fmt::Debug;

use anyhow::anyhow;
use obsidian_pb as pb;
use obsidian_util::FromProtoEnum;

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum Direction {
    Asc,
    Desc,
}

impl Debug for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Asc => f.write_str("asc"),
            Direction::Desc => f.write_str("desc"),
        }
    }
}

impl FromProtoEnum for Direction {
    fn try_from_i32(v: i32) -> anyhow::Result<Option<Self>> {
        if v == 0 {
            return Ok(None);
        }
        Ok(Some(Direction::try_from(pb::Direction::try_from(v)?)?))
    }
}

impl From<Direction> for pb::Direction {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Asc => pb::Direction::Asc,
            Direction::Desc => pb::Direction::Desc,
        }
    }
}

impl TryFrom<pb::Direction> for Direction {
    type Error = anyhow::Error;

    fn try_from(value: pb::Direction) -> Result<Self, Self::Error> {
        Ok(match value {
            pb::Direction::Unknown => return Err(anyhow!("unknown direction")),
            pb::Direction::Asc => Direction::Asc,
            pb::Direction::Desc => Direction::Desc,
        })
    }
}
