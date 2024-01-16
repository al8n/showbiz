use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;

use crate::{showbiz::Showbiz, transport::Transport, types::*};

mod r#async;
#[cfg(feature = "test")]
pub use r#async::tests::*;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Assumed header overhead
pub(crate) const COMPOUND_HEADER_OVERHEAD: usize = 2;

/// Assumed overhead per entry in compound header
pub(crate) const COMPOUND_OVERHEAD: usize = 2;

pub(crate) const USER_MSG_OVERHEAD: usize = 1;

/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[cfg(feature = "metrics")]
use sealed_metrics::*;

#[cfg(feature = "metrics")]
pub(crate) mod sealed_metrics {
  use std::sync::Once;

  static TCP_ACCEPT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_accept_counter<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // TCP_ACCEPT_COUNTER.call_once(|| {
    //   metrics::register_counter!("showbiz.tcp.accept");
    // });
    metrics::counter!("showbiz.tcp.accept", labels).increment(1);
  }

  static TCP_SENT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_sent_counter<'a>(
    val: u64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // TCP_SENT_COUNTER.call_once(|| {
    //   metrics::register_counter!("showbiz.tcp.sent");
    // });
    metrics::counter!("showbiz.tcp.sent", labels).increment(val);
  }

  static TCP_CONNECT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_connect_counter<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // TCP_CONNECT_COUNTER.call_once(|| {
    //   metrics::register_counter!("showbiz.tcp.connect");
    // });
    metrics::counter!("showbiz.tcp.connect", labels).increment(1);
  }

  static UDP_SENT_COUNTER: Once = Once::new();
  #[inline]
  pub(super) fn incr_udp_sent_counter<'a>(
    val: u64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // UDP_SENT_COUNTER.call_once(|| {
    //   metrics::register_counter!("showbiz.udp.sent");
    // });
    metrics::counter!("showbiz.udp.sent", labels).increment(val);
  }

  static LOCAL_SIZE_GAUGE: Once = Once::new();
  #[inline]
  pub(super) fn set_local_size_gauge<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // LOCAL_SIZE_GAUGE.call_once(|| {
    //   metrics::register_gauge!("showbiz.local.size");
    // });
    metrics::gauge!("showbiz.local.size", labels).increment(val);
  }

  static REMOTE_SIZE_HISTOGRAM: Once = Once::new();
  #[inline]
  pub(crate) fn add_sample_to_remote_size_histogram<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // REMOTE_SIZE_HISTOGRAM.call_once(|| {
    //   metrics::register_histogram!("showbiz.remote.size");
    // });
    metrics::histogram!("showbiz.remote.size", labels).record(val);
  }

  static NODE_INSTANCES_GAUGE: Once = Once::new();
  #[inline]
  pub(super) fn set_node_instances_gauge<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // NODE_INSTANCES_GAUGE.call_once(|| {
    //   metrics::register_gauge!("showbiz.node.instances");
    // });
    metrics::gauge!("showbiz.node.instances", labels).set(val);
  }
}

#[viewit::viewit]
pub(crate) struct RemoteServerState<'a, I, A> {
  join: bool,
  push_states: Vec<&'a PushServerState<I, A>>,
  user_state_pos: usize,
  src: Bytes,
}
