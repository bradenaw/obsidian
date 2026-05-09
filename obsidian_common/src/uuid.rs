use obsidian_pb as pb;
use uuid::Uuid;

pub fn uuid_to_proto(uuid: Uuid) -> pb::internal::Uuid {
    let (high, low) = uuid.as_u64_pair();
    pb::internal::Uuid {
        high: high,
        low: low,
    }
}

pub fn uuid_from_proto(uuid_pb: pb::internal::Uuid) -> Uuid {
    Uuid::from_u64_pair(uuid_pb.high, uuid_pb.low)
}
