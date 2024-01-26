use std::time::Duration;

use super::version::VSN_SIZE;
pub use super::version::{DelegateVersion, ProtocolVersion};

#[cfg(feature = "metrics")]
pub use memberlist_utils::MetricLabels;

/// Options used to configure the memberlist.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Options {
  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations. This is a legacy name for backwards compatibility, but
  /// should really be called StreamTimeout now that we have generalized
  /// the transport.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  timeout: Duration,

  /// The number of nodes that will be asked to perform
  /// an indirect probe of a node in the case a direct probe fails. Memberlist
  /// waits for an ack from any single indirect node, so increasing this
  /// number will increase the likelihood that an indirect probe will succeed
  /// at the expense of bandwidth.
  indirect_checks: usize,

  /// The multiplier for the number of retransmissions
  /// that are attempted for messages broadcasted over gossip. The actual
  /// count of retransmissions is calculated using the formula:
  ///
  ///   `retransmits = retransmit_mult * log(N+1)`
  ///
  /// This allows the retransmits to scale properly with cluster size. The
  /// higher the multiplier, the more likely a failed broadcast is to converge
  /// at the expense of increased bandwidth.
  retransmit_mult: usize,

  /// The multiplier for determining the time an
  /// inaccessible node is considered suspect before declaring it dead.
  /// The actual timeout is calculated using the formula:
  ///
  ///   `suspicion_timeout = suspicion_mult * log(N+1) * probe_interval`
  ///
  /// This allows the timeout to scale properly with expected propagation
  /// delay with a larger cluster size. The higher the multiplier, the longer
  /// an inaccessible node is considered part of the cluster before declaring
  /// it dead, giving that suspect node more time to refute if it is indeed
  /// still alive.
  suspicion_mult: usize,

  /// The multiplier applied to the
  /// `suspicion_timeout` used as an upper bound on detection time. This max
  /// timeout is calculated using the formula:
  ///
  /// `suspicion_max_timeout = suspicion_max_timeout_mult * suspicion_timeout`
  ///
  /// If everything is working properly, confirmations from other nodes will
  /// accelerate suspicion timers in a manner which will cause the timeout
  /// to reach the base SuspicionTimeout before that elapses, so this value
  /// will typically only come into play if a node is experiencing issues
  /// communicating with other nodes. It should be set to a something fairly
  /// large so that a node having problems will have a lot of chances to
  /// recover before falsely declaring other nodes as failed, but short
  /// enough for a legitimately isolated node to still make progress marking
  /// nodes failed in a reasonable amount of time.
  suspicion_max_timeout_mult: usize,

  /// The interval between complete state syncs.
  /// Complete state syncs are done with a single node over TCP and are
  /// quite expensive relative to standard gossiped messages. Setting this
  /// to zero will disable state push/pull syncs completely.
  ///
  /// Setting this interval lower (more frequent) will increase convergence
  /// speeds across larger clusters at the expense of increased bandwidth
  /// usage.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  probe_timeout: Duration,

  /// Set this field will turn off the fallback TCP pings that are attempted
  /// if the direct UDP ping fails. These get pipelined along with the
  /// indirect UDP pings.
  disable_tcp_pings: bool,

  /// Increase the probe interval if the node
  /// becomes aware that it might be degraded and not meeting the soft real
  /// time requirements to reliably probe other nodes.
  awareness_max_multiplier: usize,
  /// The interval between sending messages that need
  /// to be gossiped that haven't been able to piggyback on probing messages.
  /// If this is set to zero, non-piggyback gossip is disabled. By lowering
  /// this value (more frequent) gossip messages are propagated across
  /// the cluster more quickly at the expense of increased bandwidth.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  gossip_interval: Duration,
  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  gossip_to_the_dead_time: Duration,

  /// Used to guarantee protocol-compatibility
  protocol_version: ProtocolVersion,

  // #[viewit(getter(style = "ref", result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeyring>")))]
  // secret_keyring: Option<SecretKeyring>,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_version: DelegateVersion,

  /// Size of Memberlist's internal channel which handles UDP messages. The
  /// size of this determines the size of the queue which Memberlist will keep
  /// while UDP messages are handled.
  handoff_queue_depth: usize,

  /// Controls the time before a dead node's name can be
  /// reclaimed by one with a different address or port. By default, this is 0,
  /// meaning nodes cannot be reclaimed this way.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  dead_node_reclaim_time: Duration,

  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  queue_check_interval: Duration,

  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(
        doc = "Get the metric labels for the memberlist.",
        cfg(feature = "metrics"),
        cfg_attr(docsrs, doc(cfg(feature = "metrics")))
      )
    ),
    setter(attrs(
      doc = "Sets the metric labels for the memberlist.",
      cfg(feature = "metrics"),
      cfg_attr(docsrs, doc(cfg(feature = "metrics")))
    ))
  )]
  #[cfg(feature = "metrics")]
  metric_labels: std::sync::Arc<MetricLabels>,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::lan()
  }
}

impl Options {
  #[inline]
  pub const fn build_vsn_array(&self) -> [u8; VSN_SIZE] {
    [self.protocol_version as u8, self.delegate_version as u8]
  }

  /// Returns a sane set of configurations for Memberlist.
  /// It uses the hostname as the node name, and otherwise sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[inline]
  pub fn lan() -> Self {
    Self {
      timeout: Duration::from_secs(10), // Timeout after 10 seconds
      indirect_checks: 3,               // Use 3 nodes for the indirect ping
      retransmit_mult: 4,               // Retransmit a message 4 * log(N+1) nodes
      suspicion_mult: 4,                // Suspect a node for 4 * log(N+1) * Interval
      suspicion_max_timeout_mult: 6,    // For 10k nodes this will give a max timeout of 120 seconds
      push_pull_interval: Duration::from_secs(30), // Low frequency
      probe_interval: Duration::from_millis(500), // Failure check every second
      probe_timeout: Duration::from_secs(1), // Reasonable RTT time for LAN
      disable_tcp_pings: false,         // TCP pings are safe, even with mixed versions
      awareness_max_multiplier: 8,      // Probe interval backs off to 8 seconds
      gossip_interval: Duration::from_millis(200), // Gossip every 200ms
      gossip_nodes: 3,                  // Gossip to 3 nodes
      gossip_to_the_dead_time: Duration::from_secs(30), // same as push/pull
      delegate_version: DelegateVersion::V0,
      protocol_version: ProtocolVersion::V0,
      handoff_queue_depth: 1024,
      dead_node_reclaim_time: Duration::ZERO,
      queue_check_interval: Duration::from_secs(30),
      #[cfg(feature = "metrics")]
      metric_labels: std::sync::Arc::new(MetricLabels::new()),
    }
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn wan() -> Self {
    Self::lan()
      .with_timeout(Duration::from_secs(30))
      .with_suspicion_mult(6)
      .with_push_pull_interval(Duration::from_secs(60))
      .with_probe_timeout(Duration::from_secs(3))
      .with_probe_interval(Duration::from_secs(5))
      .with_gossip_nodes(4)
      .with_gossip_interval(Duration::from_millis(500))
      .with_gossip_to_the_dead_time(Duration::from_secs(60))
  }

  /// Returns a configuration
  /// that is optimized for a local loopback environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn local() -> Self {
    Self::lan()
      .with_timeout(Duration::from_secs(1))
      .with_indirect_checks(1)
      .with_retransmit_mult(2)
      .with_suspicion_mult(3)
      .with_push_pull_interval(Duration::from_secs(15))
      .with_probe_timeout(Duration::from_millis(200))
      .with_probe_interval(Duration::from_secs(1))
      .with_gossip_interval(Duration::from_millis(100))
      .with_gossip_to_the_dead_time(Duration::from_secs(15))
  }
}

// #[derive(Debug, Clone, Copy)]
// pub struct ForbiddenIp(IpAddr);

// impl core::fmt::Display for ForbiddenIp {
//   fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//     write!(f, "IP {} is not allowed", self.0)
//   }
// }

// impl std::error::Error for ForbiddenIp {}
