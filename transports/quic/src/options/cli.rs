use clap::ArgMatches;

use super::*;

// /// Used to configure a net transport.
// #[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
// #[derive(Debug)]
// #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// #[cfg_attr(
//   feature = "serde",
//   serde(bound(
//     serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize, A::Options: serde::Serialize, S::Options: serde::Serialize",
//     deserialize = "I: serde::Deserialize<'de>, A: AddressResolver, A::Address: serde::Deserialize<'de>, A::ResolvedAddress: serde::Deserialize<'de>, A::Options: serde::Deserialize<'de>, S::Options: serde::Deserialize<'de>"
//   ))
// )]
// pub struct QuicTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
// {
//   /// The local node's ID.
//   #[viewit(
//     getter(const, style = "ref", attrs(doc = "Get the id of the node."),),
//     setter(attrs(doc = "Set the id of the node. (Builder pattern)"),)
//   )]
//   id: I,

//   /// A set of addresses to bind to for both TCP and UDP
//   /// communications.
//   #[viewit(
//     getter(
//       style = "ref",
//       const,
//       attrs(doc = "Get a list of addresses to bind to for QUIC communications."),
//     ),
//     setter(attrs(
//       doc = "Set the list of addresses to bind to for QUIC communications. (Builder pattern)"
//     ),)
//   )]
//   bind_addresses: IndexSet<A::Address>,

//   /// Label is an optional set of bytes to include on the outside of each
//   /// packet and stream.
//   ///
//   /// If gossip encryption is enabled and this is set it is treated as GCM
//   /// authenticated data.
//   #[viewit(
//     getter(const, style = "ref", attrs(doc = "Get the label of the node."),),
//     setter(attrs(doc = "Set the label of the node. (Builder pattern)"),)
//   )]
//   label: Label,

//   /// Resolver options, which used to construct the address resolver for this transport.
//   #[viewit(
//     getter(const, style = "ref", attrs(doc = "Get the address resolver options."),),
//     setter(attrs(doc = "Set the address resolver options. (Builder pattern)"),)
//   )]
//   resolver: A::Options,

//   /// Stream layer options, which used to construct the stream layer for this transport.
//   #[viewit(
//     getter(const, style = "ref", attrs(doc = "Get the stream layer options."),),
//     setter(attrs(doc = "Set the stream layer options. (Builder pattern)"),)
//   )]
//   stream_layer: S::Options,

//   /// Skips the check that inbound packets and gossip
//   /// streams need to be label prefixed.
//   #[viewit(
//     getter(
//       const,
//       attrs(
//         doc = "Get if the check that inbound packets and gossip streams need to be label prefixed."
//       ),
//     ),
//     setter(attrs(
//       doc = "Set if the check that inbound packets and gossip streams need to be label prefixed. (Builder pattern)"
//     ),)
//   )]
//   skip_inbound_label_check: bool,

//   /// The timeout used for I/O
//   #[cfg_attr(feature = "serde", serde(with = "humantime_serde::option"))]
//   #[viewit(
//     getter(const, attrs(doc = "Get timeout used for I/O."),),
//     setter(attrs(doc = "Set timeout used for I/O. (Builder pattern)"),)
//   )]
//   timeout: Option<Duration>,

//   /// The period of time to cleanup the connection pool.
//   #[cfg_attr(
//     feature = "serde",
//     serde(
//       with = "humantime_serde",
//       default = "default_connection_pool_cleanup_period"
//     )
//   )]
//   #[viewit(
//     getter(const, attrs(doc = "Get the cleanup period for the connection pool."),),
//     setter(attrs(doc = "Set the cleanup period for the connection pool"),)
//   )]
//   connection_pool_cleanup_period: Duration,

//   /// The time to live for each connection in the connection pool. Default is `None`.
//   #[viewit(
//     getter(
//       const,
//       attrs(doc = "Get the time to live for each connection in the connection pool."),
//     ),
//     setter(attrs(doc = "Set the time to live for each connection for the connection pool"),)
//   )]
//   connection_ttl: Option<Duration>,

//   /// Policy for Classless Inter-Domain Routing (CIDR).
//   ///
//   /// By default, allow any connection
//   #[cfg_attr(feature = "serde", serde(default))]
//   #[viewit(
//     getter(
//       const,
//       style = "ref",
//       attrs(doc = "Get the policy for Classless Inter-Domain Routing (CIDR)."),
//     ),
//     setter(attrs(
//       doc = "Set the policy for Classless Inter-Domain Routing (CIDR). (Builder pattern)"
//     ),)
//   )]
//   cidrs_policy: CIDRsPolicy,

//   /// Used to control message compression. This can
//   /// be used to reduce bandwidth usage at the cost of slightly more CPU
//   /// utilization.
//   #[cfg(feature = "compression")]
//   #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
//   #[viewit(
//     getter(
//       const,
//       attrs(
//         doc = "Get the compression algorithm used for outgoing.",
//         cfg(feature = "compression"),
//         cfg_attr(docsrs, doc(cfg(feature = "compression")))
//       ),
//     ),
//     setter(attrs(
//       doc = "Set the compression algorithm used for outgoing. (Builder pattern)",
//       cfg(feature = "compression"),
//       cfg_attr(docsrs, doc(cfg(feature = "compression")))
//     ),)
//   )]
//   compressor: Option<Compressor>,

//   /// The size of a message that should be offload to [`rayon`] thread pool
//   /// for encryption or compression.
//   ///
//   /// The default value is 1KB, which means that any message larger than 1KB
//   /// will be offloaded to [`rayon`] thread pool for encryption or compression.
//   #[cfg(feature = "compression")]
//   #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
//   #[viewit(
//     getter(
//       const,
//       attrs(
//         doc = "Get the size of a message that should be offload to [`rayon`] thread pool for encryption or compression.",
//         cfg(feature = "compression"),
//         cfg_attr(docsrs, doc(cfg(feature = "compression")))
//       ),
//     ),
//     setter(attrs(
//       doc = "Set the size of a message that should be offload to [`rayon`] thread pool for encryption or compression. (Builder pattern)",
//       cfg(feature = "compression"),
//       cfg_attr(docsrs, doc(cfg(feature = "compression")))
//     ),)
//   )]
//   offload_size: usize,

//   /// The metrics labels.
//   #[cfg(feature = "metrics")]
//   #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
//   #[viewit(
//     getter(
//       style = "ref",
//       result(
//         converter(fn = "Option::as_deref"),
//         type = "Option<&memberlist_core::types::MetricLabels>"
//       ),
//       attrs(
//         doc = "Get the metrics labels.",
//         cfg(feature = "metrics"),
//         cfg_attr(docsrs, doc(cfg(feature = "metrics")))
//       ),
//     ),
//     setter(attrs(
//       doc = "Set the metrics labels. (Builder pattern)",
//       cfg(feature = "metrics"),
//       cfg_attr(docsrs, doc(cfg(feature = "metrics")))
//     ))
//   )]
//   metric_labels: Option<Arc<memberlist_core::types::MetricLabels>>,
// }

impl<I, A, S> clap::FromArgMatches for QuicTransportOptions<I, A, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
{
  fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
    matches.
  }

  fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
    todo!()
  }
}

impl<I, A, S> clap::Args for QuicTransportOptions<I, A, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
{
  fn augment_args(cmd: clap::Command) -> clap::Command {
    todo!()
  }

  fn augment_args_for_update(cmd: clap::Command) -> clap::Command {
    todo!()
  }
}
