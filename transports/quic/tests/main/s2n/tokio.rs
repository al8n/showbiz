use super::*;
use agnostic::tokio::TokioRuntime;
use memberlist_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[path = "tokio/handle_ping.rs"]
mod handle_ping;

#[path = "tokio/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "tokio/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "tokio/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "tokio/piggyback.rs"]
mod piggyback;

#[path = "tokio/send.rs"]
mod send;

#[path = "tokio/join.rs"]
mod join;

#[path = "tokio/promised_ping.rs"]
mod promised_ping;

#[path = "tokio/promised_push_pull.rs"]
mod promised_push_pull;
