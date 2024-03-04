use agnostic::smol::SmolRuntime;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `smol` runtime.
#[cfg(all(
  any(feature = "tcp", feature = "tls", feature = "native-tls"),
  not(target_family = "wasm")
))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(
    any(feature = "tcp", feature = "tls", feature = "native-tls"),
    not(target_family = "wasm")
  )))
)]
pub type SmolTcpMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tcp::Tcp<SmolRuntime>,
    W,
    SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `smol` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type SmolTlsMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tls::Tls<SmolRuntime>,
    W,
    SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`NativeTls`](memberlist_net::stream_layer::native_tls::NativeTls) stream layer with `smol` runtime.
#[cfg(all(feature = "native-tls", not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(feature = "native-tls", not(target_family = "wasm"))))
)]
pub type SmolNativeTlsMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::native_tls::NativeTls<SmolRuntime>,
    W,
    SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `smol` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type SmolQuicMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_quic::QuicTransport<
    I,
    A,
    memberlist_quic::stream_layer::quinn::Quinn<SmolRuntime>,
    W,
    SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`S2n`](memberlist_quic::stream_layer::s2n::S2n) stream layer with `smol` runtime.
#[cfg(all(feature = "s2n", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "s2n", not(target_family = "wasm")))))]
pub type SmolS2nMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_quic::QuicTransport<
    I,
    A,
    memberlist_quic::stream_layer::s2n::S2n<SmolRuntime>,
    W,
    SmolRuntime,
  >,
  D,
>;
