use std::{
  collections::HashSet,
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  path::PathBuf,
  time::Duration,
};

use bytes::Bytes;

use super::{
  keyring::SecretKey,
  security::{EncryptionAlgo, MAX_ENCRYPTION_VERSION},
  types::{CompressionAlgo, Name},
};

#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Options {
  /// The name of this node. This must be unique in the cluster.
  #[viewit(getter(const, style = "ref"))]
  name: Name,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  #[viewit(getter(const, style = "ref"))]
  label: Bytes,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  skip_inbound_label_check: bool,

  /// Configuration related to what address to bind to and ports to
  /// listen on. The port is used for both UDP and TCP gossip. It is
  /// assumed other nodes are running on this port, but they do not need
  /// to.
  bind_addr: SocketAddr,

  /// Configuration related to what address to advertise to other
  /// cluster members. Used for nat traversal.
  advertise_addr: Option<SocketAddr>,

  /// The configured encryption type that we
  /// will _speak_. This must be between [`EncryptionAlgo::MIN`] and
  /// [`EncryptionAlgo::MAX`].
  encryption_algo: EncryptionAlgo,

  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations. This is a legacy name for backwards compatibility, but
  /// should really be called StreamTimeout now that we have generalized
  /// the transport.
  tcp_timeout: Duration,

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
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
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
  gossip_interval: Duration,
  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  gossip_to_the_dead_time: Duration,
  /// Controls whether to enforce encryption for incoming
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  gossip_verify_incoming: bool,
  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  gossip_verify_outgoing: bool,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  compression_algo: CompressionAlgo,

  /// Used to initialize the primary encryption key in a keyring.
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  /// The value should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  secret_key: Option<SecretKey>,

  // /// Holds all of the encryption keys used internally. It is
  // /// automatically initialized using the SecretKey and SecretKeys values.
  // #[viewit(getter(style = "ref", result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeyring>")))]
  // secret_keyring: Option<SecretKeyring>,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_version: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_min: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_max: u8,

  /// Points to the system's DNS config file, usually located
  /// at `/etc/resolv.conf`. It can be overridden via config for easier testing.
  #[viewit(getter(const, style = "ref"))]
  dns_config_path: PathBuf,

  /// Size of Memberlist's internal channel which handles UDP messages. The
  /// size of this determines the size of the queue which Memberlist will keep
  /// while UDP messages are handled.
  handoff_queue_depth: usize,
  /// Maximum number of bytes that memberlist will put in a packet (this
  /// will be for UDP packets by default with a NetTransport). A safe value
  /// for this is typically 1400 bytes (which is the default). However,
  /// depending on your network's MTU (Maximum Transmission Unit) you may
  /// be able to increase this to get more content into each gossip packet.
  packet_buffer_size: usize,

  /// Controls the time before a dead node's name can be
  /// reclaimed by one with a different address or port. By default, this is 0,
  /// meaning nodes cannot be reclaimed this way.
  dead_node_reclaim_time: Duration,

  /// Controls if the name of a node is required when sending
  /// a message to that node.
  require_node_names: bool,

  /// If [`None`], allow any connection (default), otherwise specify all networks
  /// allowed to connect (you must specify IPv6/IPv4 separately)
  /// Using an empty Vec will block all connections.
  #[viewit(getter(
    style = "ref",
    result(
      converter(fn = "Option::as_ref"),
      type = "Option<&HashSet<ipnet::IpNet>>"
    )
  ))]
  allowed_cidrs: Option<HashSet<ipnet::IpNet>>,
  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  queue_check_interval: Duration,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::lan()
  }
}

impl Options {
  #[inline]
  pub const fn build_vsn_array(&self) -> [u8; 6] {
    [
      MAX_ENCRYPTION_VERSION as u8,
      MAX_ENCRYPTION_VERSION as u8,
      self.encryption_algo as u8,
      self.delegate_protocol_min,
      self.delegate_protocol_max,
      self.delegate_protocol_version,
    ]
  }

  /// Returns a sane set of configurations for Memberlist.
  /// It uses the hostname as the node name, and otherwise sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[inline]
  pub fn lan() -> Self {
    #[cfg(not(any(target_arch = "wasm32", windows)))]
    let hostname = {
      let uname = rustix::process::uname();
      uname
        .nodename()
        .to_string_lossy()
        .to_string()
        .try_into()
        .unwrap_or_default()
    };

    #[cfg(windows)]
    let hostname = {
      match hostname::get() {
        Ok(name) => name.to_string_lossy().into(),
        Err(_) => "".into(),
      }
    };

    #[cfg(target_arch = "wasm32")]
    let hostname = "".into();

    Self {
      name: hostname,
      label: Default::default(),
      skip_inbound_label_check: false,
      bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 7946)),
      advertise_addr: None,
      encryption_algo: EncryptionAlgo::MAX,
      tcp_timeout: Duration::from_secs(10), // Timeout after 10 seconds
      indirect_checks: 3,                   // Use 3 nodes for the indirect ping
      retransmit_mult: 4,                   // Retransmit a message 4 * log(N+1) nodes
      suspicion_mult: 4,                    // Suspect a node for 4 * log(N+1) * Interval
      suspicion_max_timeout_mult: 6, // For 10k nodes this will give a max timeout of 120 seconds
      push_pull_interval: Duration::from_secs(30), // Low frequency
      probe_interval: Duration::from_millis(500), // Failure check every second
      probe_timeout: Duration::from_secs(1), // Reasonable RTT time for LAN
      disable_tcp_pings: false,      // TCP pings are safe, even with mixed versions
      awareness_max_multiplier: 8,   // Probe interval backs off to 8 seconds
      gossip_interval: Duration::from_millis(200), // Gossip every 200ms
      gossip_nodes: 3,               // Gossip to 3 nodes
      gossip_to_the_dead_time: Duration::from_secs(30), // same as push/pull
      gossip_verify_incoming: true,
      gossip_verify_outgoing: true,
      compression_algo: CompressionAlgo::LZW, // Enable compression by default
      secret_key: None,
      delegate_protocol_version: 0,
      delegate_protocol_min: 0,
      delegate_protocol_max: 0,
      dns_config_path: PathBuf::from("/etc/resolv.conf"),
      handoff_queue_depth: 1024,
      packet_buffer_size: 1400,
      dead_node_reclaim_time: Duration::ZERO,
      require_node_names: false,
      allowed_cidrs: None,
      queue_check_interval: Duration::from_secs(30),
    }
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn wan() -> Self {
    Self::lan()
      .with_tcp_timeout(Duration::from_secs(30))
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
      .with_tcp_timeout(Duration::from_secs(1))
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
