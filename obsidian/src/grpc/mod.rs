//! GRPC translates the traits in [`crate::runtime`] over the network.

#![allow(unused_imports)]

mod gateway_client;
mod gateway_server;
mod grpc_nodes;
mod journals_client;
mod journals_server;
mod node_client;
mod node_server;
#[cfg(test)]
mod node_tests;
mod util;

pub(crate) use crate::grpc::gateway_client::GatewayClient;
pub(crate) use crate::grpc::gateway_server::GatewayServer;
pub(crate) use crate::grpc::grpc_nodes::GrpcNodes;
pub(crate) use crate::grpc::journals_client::JournalsClient;
pub(crate) use crate::grpc::journals_server::JournalsServer;
pub(crate) use crate::grpc::node_client::NodeClient;
pub(crate) use crate::grpc::node_server::NodeServer;
