use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

use anyhow::anyhow;

use crate::pb;

pub(crate) fn ip_addr_to_proto(addr: IpAddr) -> pb::internal::IpAddr {
    pb::internal::IpAddr {
        addr_type: Some(match addr {
            IpAddr::V4(v4) => pb::internal::ip_addr::AddrType::V4(pb::internal::Ipv4Addr {
                bits: u32::from_be_bytes(v4.octets()),
            }),
            IpAddr::V6(v6) => {
                let octets = v6.octets();
                let a = {
                    let mut a = [0u8; 8];
                    a.copy_from_slice(&octets[..8]);
                    u64::from_be_bytes(a)
                };
                let b = {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&octets[8..]);
                    u64::from_be_bytes(b)
                };

                pb::internal::ip_addr::AddrType::V6(pb::internal::Ipv6Addr { a, b })
            }
        }),
    }
}

pub(crate) fn ip_addr_from_proto(addr_pb: pb::internal::IpAddr) -> anyhow::Result<IpAddr> {
    Ok(
        match addr_pb
            .addr_type
            .ok_or_else(|| anyhow!("addr_type missing"))?
        {
            pb::internal::ip_addr::AddrType::V4(v4_pb) => {
                IpAddr::V4(Ipv4Addr::from_octets(v4_pb.bits.to_be_bytes()))
            }
            pb::internal::ip_addr::AddrType::V6(v6_pb) => {
                let mut octets = [0u8; 16];
                let a = v6_pb.a.to_be_bytes();
                octets[..8].copy_from_slice(&a);
                let b = v6_pb.b.to_be_bytes();
                octets[8..].copy_from_slice(&b);
                IpAddr::V6(Ipv6Addr::from_octets(octets))
            }
        },
    )
}
