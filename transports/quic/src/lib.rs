//! [`Transport`](memberlist_core::Transport)'s network transport layer based on QUIC.
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use futures::FutureExt;
use memberlist_core::{
  transport::{
    stream::{
      packet_stream, promised_stream, PacketProducer, PacketSubscriber, StreamProducer,
      StreamSubscriber,
    },
    TimeoutableReadStream, Transport, TransportError, Wire,
  },
  types::{Message, Packet},
};
use memberlist_utils::{net::CIDRsPolicy, Label, LabelError, OneOrMore, SmallVec, TinyVec};
use nodecraft::{resolver::AddressResolver, CheapClone, Id};
use pollster::FutureExt as _;
use wg::AsyncWaitGroup;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;
#[cfg(feature = "compression")]
use compressor::*;

mod error;
pub use error::*;
mod io;
mod options;
pub use options::*;
mod stream_layer;
pub use stream_layer::*;

const DEFAULT_PORT: u16 = 7946;

const MAX_MESSAGE_LEN_SIZE: usize = core::mem::size_of::<u32>();
const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;
// compound tag + MAX_MESSAGE_LEN_SIZE
const PACKET_HEADER_OVERHEAD: usize = 1 + 1 + MAX_MESSAGE_LEN_SIZE;
const PACKET_OVERHEAD: usize = MAX_MESSAGE_LEN_SIZE;
const NUM_PACKETS_PER_BATCH: usize = 255;
const HEADER_SIZE: usize = 1 + MAX_MESSAGE_LEN_SIZE;

#[cfg(feature = "compression")]
const COMPRESS_HEADER: usize = 1 + MAX_MESSAGE_LEN_SIZE;

const MAX_INLINED_BYTES: usize = 64;

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  opts: Arc<QuicTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  #[allow(dead_code)]
  stream_layer: Arc<S>,
  connectors: SmallVec<S::Connector>,
  round_robin: AtomicUsize,

  wg: AsyncWaitGroup,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,

  max_payload_size: usize,
  _marker: PhantomData<W>,
}

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  /// Creates a new quic transport.
  pub async fn new(
    resolver: A,
    stream_layer: S,
    opts: QuicTransportOptions<I, A>,
  ) -> Result<Self, QuicTransportError<A, S, W>> {
    match opts.bind_port {
      Some(0) | None => Self::retry(resolver, stream_layer, 10, opts).await,
      _ => Self::retry(resolver, stream_layer, 1, opts).await,
    }
  }

  async fn new_in(
    resolver: Arc<A>,
    stream_layer: Arc<S>,
    opts: Arc<QuicTransportOptions<I, A>>,
  ) -> Result<Self, QuicTransportError<A, S, W>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addresses.is_empty() {
      return Err(QuicTransportError::EmptyBindAddresses);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut connectors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut acceptors = SmallVec::with_capacity(opts.bind_addresses.len());
    let bind_port = opts.bind_port.unwrap_or(0);

    for &addr in opts.bind_addresses.iter() {
      let addr = SocketAddr::new(addr, bind_port);
      let (acceptor, connector) = match stream_layer.bind(addr).await {
        Ok(res) => res,
        Err(e) => return Err(QuicTransportError::ListenPromised(addr, e)),
      };
      connectors.push(connector);
      acceptors.push(acceptor);
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let endpoint1_addr = acceptors[0].0;

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for (local_addr, bi_acceptor, uni_acceptor) in acceptors {
      Processor::<A, Self, S> {
        wg: wg.clone(),
        bi: bi_acceptor,
        uni: uni_acceptor,
        packet_tx: packet_tx.clone(),
        stream_tx: stream_tx.clone(),
        label: opts.label.clone(),
        local_addr,
        shutdown: shutdown.clone(),
        timeout: opts.timeout,
        shutdown_rx: shutdown_rx.clone(),
        skip_inbound_label_check: opts.skip_inbound_label_check,
        #[cfg(feature = "compression")]
        offload_size: opts.offload_size,
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
      }
      .run();
    }

    // find final advertise address
    let advertise_addr = match opts.advertise_address {
      Some(addr) => addr,
      None => {
        let addr = if opts.bind_addresses[0].is_unspecified() {
          local_ip_address::local_ip().map_err(|e| match e {
            local_ip_address::Error::LocalIpAddressNotFound => QuicTransportError::NoPrivateIP,
            e => QuicTransportError::NoInterfaceAddresses(e),
          })?
        } else {
          endpoint1_addr.ip()
        };

        // Use the port we are bound to.
        SocketAddr::new(addr, endpoint1_addr.port())
      }
    };

    Ok(Self {
      advertise_addr,
      max_payload_size: MAX_MESSAGE_SIZE.min(stream_layer.max_stream_data()),
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      connectors,
      round_robin: AtomicUsize::new(0),

      stream_layer,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  async fn retry(
    resolver: A,
    stream_layer: S,
    limit: usize,
    opts: QuicTransportOptions<I, A>,
  ) -> Result<Self, QuicTransportError<A, S, W>> {
    let mut i = 0;
    let resolver = Arc::new(resolver);
    let stream_layer = Arc::new(stream_layer);
    let opts = Arc::new(opts);
    loop {
      let transport = { Self::new_in(resolver.clone(), stream_layer.clone(), opts.clone()).await };

      match transport {
        Ok(t) => {
          if let Some(0) | None = opts.bind_port {
            let port = t.advertise_addr.port();
            tracing::warn!(target:  "memberlist.transport.quic", "using dynamic bind port {port}");
          }
          return Ok(t);
        }
        Err(e) => {
          tracing::debug!(target="memberlist.transport.quic", err=%e, "fail to create transport");
          if i == limit - 1 {
            return Err(e);
          }
          i += 1;
        }
      }
    }
  }
}

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1 + core::mem::size_of::<u32>();
    }

    overhead
  }

  #[inline]
  fn next_connector(&self) -> &S::Connector {
    let idx = self
      .round_robin
      .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
      % self.connectors.len();
    &self.connectors[idx]
  }
}

struct Batch<I, A> {
  num_packets: usize,
  packets: TinyVec<Message<I, A>>,
  estimate_encoded_len: usize,
}

impl<I, A> Batch<I, A> {
  fn estimate_encoded_len(&self) -> usize {
    if self.packets.len() == 1 {
      return self.estimate_encoded_len - PACKET_OVERHEAD;
    }
    self.estimate_encoded_len
  }
}

impl<I, A, S, W> Transport for QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  type Error = QuicTransportError<A, S, W>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = A::Runtime;

  async fn resolve(
    &self,
    addr: &<Self::Resolver as AddressResolver>::Address,
  ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
    self
      .resolver
      .resolve(addr)
      .await
      .map_err(|e| Self::Error::Resolve {
        addr: addr.cheap_clone(),
        err: e,
      })
  }

  #[inline(always)]
  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  #[inline(always)]
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.opts.address
  }

  #[inline(always)]
  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  #[inline(always)]
  fn max_payload_size(&self) -> usize {
    self.max_payload_size
  }

  #[inline(always)]
  fn packet_overhead(&self) -> usize {
    PACKET_OVERHEAD
  }

  #[inline(always)]
  fn packets_header_overhead(&self) -> usize {
    self.fix_packet_overhead() + PACKET_HEADER_OVERHEAD
  }

  fn blocked_address(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    let ip = addr.ip();
    if self.opts.cidrs_policy.is_blocked(&ip) {
      Err(Self::Error::BlockedIp(ip))
    } else {
      Ok(())
    }
  }

  async fn read_message(
    &self,
    _from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    let mut tag = [0u8; 2];

    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    let mut stream_label = if tag[0] == Label::TAG {
      let label_size = tag[1] as usize;
      // consume peeked
      conn.read_exact(&mut tag).await.unwrap();

      let mut label = vec![0u8; label_size];
      conn
        .read_exact(&mut label)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;

      Label::try_from(Bytes::from(label)).map_err(|e| QuicTransportError::Label(e.into()))?
    } else {
      Label::empty()
    };

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target: "memberlist.transport.quic.read_message", "unexpected double stream label header");
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(target: "memberlist.transport.quic.read_message", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), stream_label).into());
    }

    let readed = stream_label.encoded_overhead();

    #[cfg(not(feature = "compression"))]
    return self
      .read_message_without_compression(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(feature = "compression")]
    self
      .read_message_with_compression(conn)
      .await
      .map(|(read, msg)| (readed + read, msg))
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    #[cfg(not(feature = "compression"))]
    return self.send_message_without_comression(conn, msg).await;

    #[cfg(feature = "compression")]
    self.send_message_with_compression(conn, msg).await
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    let start = Instant::now();
    let encoded_size = W::encoded_len(&packet);
    self
      .send_batch(
        *addr,
        Batch {
          packets: TinyVec::from(packet),
          num_packets: 1,
          estimate_encoded_len: self.packets_header_overhead() + PACKET_OVERHEAD + encoded_size,
        },
      )
      .await
      .map(|sent| (sent, start))
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    let start = Instant::now();

    let mut batches =
      SmallVec::<Batch<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>::new();
    let packets_overhead = self.packets_header_overhead();
    let mut estimate_batch_encoded_size = 0;
    let mut current_packets_in_batch = 0;

    // get how many packets a batch
    for packet in packets.iter() {
      let ep_len = W::encoded_len(packet);
      // check if we reach the maximum packet size
      let current_encoded_size = ep_len + estimate_batch_encoded_size;
      if current_encoded_size >= self.max_payload_size()
        || current_packets_in_batch >= NUM_PACKETS_PER_BATCH
      {
        batches.push(Batch {
          packets: TinyVec::with_capacity(current_packets_in_batch),
          num_packets: current_packets_in_batch,
          estimate_encoded_len: estimate_batch_encoded_size,
        });
        estimate_batch_encoded_size =
          packets_overhead + PACKET_HEADER_OVERHEAD + PACKET_OVERHEAD + ep_len;
        current_packets_in_batch = 1;
      } else {
        estimate_batch_encoded_size += PACKET_OVERHEAD + ep_len;
        current_packets_in_batch += 1;
      }
    }

    // consume the packets to small batches according to batch_offsets.

    // if batch_offsets is empty, means that packets can be sent by one I/O call
    if batches.is_empty() {
      self
        .send_batch(
          *addr,
          Batch {
            num_packets: packets.len(),
            packets,
            estimate_encoded_len: estimate_batch_encoded_size,
          },
        )
        .await
        .map(|sent| (sent, start))
    } else {
      let mut batch_idx = 0;
      for (idx, packet) in packets.into_iter().enumerate() {
        let batch = &mut batches[batch_idx];
        batch.packets.push(packet);
        if batch.num_packets == idx - 1 {
          batch_idx += 1;
        }
      }

      let mut total_bytes_sent = 0;
      let resps =
        futures::future::join_all(batches.into_iter().map(|b| self.send_batch(*addr, b))).await;

      for res in resps {
        match res {
          Ok(sent) => {
            total_bytes_sent += sent;
          }
          Err(e) => return Err(e),
        }
      }
      Ok((total_bytes_sent, start))
    }
  }

  async fn dial_timeout(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    timeout: std::time::Duration,
  ) -> Result<Self::Stream, Self::Error> {
    self
      .next_connector()
      .open_bi_with_timeout(*addr, timeout)
      .await
      .map_err(|e| Self::Error::Stream(e.into()))
  }

  fn packet(
    &self,
  ) -> PacketSubscriber<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress> {
    self.packet_rx.clone()
  }

  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream> {
    self.stream_rx.clone()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    if self.shutdown_tx.is_closed() {
      return Ok(());
    }

    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();

    for connector in self.connectors.iter() {
      if let Err(e) = connector
        .close()
        .await
        .map_err(|e| Self::Error::Stream(e.into()))
      {
        tracing::error!(target: "memberlist.transport.quic", err = %e, "failed to close connector");
      }
    }

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }
}

impl<I, A, S, W> Drop for QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn drop(&mut self) {
    if self.shutdown_tx.is_closed() {
      return;
    }
    let _ = self.shutdown().block_on();
  }
}

struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer,
> {
  label: Label,
  local_addr: SocketAddr,
  uni: S::UniAcceptor,
  bi: S::BiAcceptor,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,

  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,

  skip_inbound_label_check: bool,
  timeout: Option<Duration>,
  wg: AsyncWaitGroup,

  #[cfg(feature = "compression")]
  offload_size: usize,

  #[cfg(feature = "metrics")]
  metric_labels: Arc<memberlist_utils::MetricLabels>,
}

impl<A, T, S> Processor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream>,
  S: StreamLayer,
{
  fn run(self) {
    let Self {
      bi,
      uni,
      packet_tx,
      stream_tx,
      shutdown_rx,
      shutdown,
      local_addr,
      label,
      skip_inbound_label_check,
      timeout,
      wg,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    let pwg = wg.add(1);
    let pshutdown = shutdown.clone();
    let pshutdown_rx = shutdown_rx.clone();
    <T::Runtime as Runtime>::spawn_detach(async move {
      Self::listen_packet(
        local_addr,
        label,
        uni,
        packet_tx,
        pshutdown,
        pshutdown_rx,
        skip_inbound_label_check,
        timeout,
        #[cfg(feature = "compression")]
        offload_size,
        #[cfg(feature = "metrics")]
        metric_labels,
      )
      .await;
      pwg.done();
    });

    let swg = wg.add(1);
    <T::Runtime as Runtime>::spawn_detach(async move {
      Self::listen_stream(local_addr, bi, stream_tx, shutdown, shutdown_rx).await;
      swg.done();
    });
  }

  async fn listen_stream(
    local_addr: SocketAddr,
    acceptor: S::BiAcceptor,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: async_channel::Receiver<()>,
  ) {
    tracing::info!(
      target: "memberlist.transport.quic",
      "listening stream on {local_addr}"
    );

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
          return;
        }
        incoming = acceptor.accept_bi().fuse() => {
          match incoming {
            Ok((stream, remote_addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;
              if let Err(e) = stream_tx
                .send(remote_addr, stream)
                .await
              {
                tracing::error!(target:  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "failed to send stream connection");
              }
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
                return;
              }

              if loop_delay == Duration::ZERO {
                loop_delay = BASE_DELAY;
              } else {
                loop_delay *= 2;
              }

              if loop_delay > MAX_DELAY {
                loop_delay = MAX_DELAY;
              }

              tracing::error!(target:  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "error accepting stream connection");
              <T::Runtime as Runtime>::sleep(loop_delay).await;
              continue;
            }
          }
        }
      }
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn listen_packet(
    local_addr: SocketAddr,
    label: Label,
    acceptor: S::UniAcceptor,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: async_channel::Receiver<()>,
    skip_inbound_label_check: bool,
    timeout: Option<Duration>,
    #[cfg(feature = "compression")] offload_size: usize,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_utils::MetricLabels>,
  ) {
    tracing::info!(
      target: "memberlist.transport.quic",
      "listening packet on {local_addr}"
    );
    loop {
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown packet listener");
          return;
        }
        incoming = acceptor.accept_uni().fuse() => {
          match incoming {
            Ok((mut recv_stream, remote_addr)) => {
              let start = Instant::now();
              recv_stream.set_read_timeout(timeout);

              let (read, msg) = match Self::handle_packet(
                recv_stream,
                &label,
                skip_inbound_label_check,
                #[cfg(feature = "compression")] offload_size,
              ).await {
                Ok(msg) => msg,
                Err(e) => {
                  tracing::error!(target: "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "fail to handle UDP packet");
                  continue;
                }
              };

              #[cfg(feature = "metrics")]
              {
                metrics::counter!("memberlist.packet.bytes.processing", metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
              }

              if let Err(e) = packet_tx.send(Packet::new(msg, remote_addr, start)).await {
                tracing::error!(target: "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "failed to send packet");
              }

              #[cfg(feature = "metrics")]
              metrics::counter!("memberlist.packet.received", metric_labels.iter()).increment(read as u64);
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
                return;
              }

              tracing::error!(target: "memberlist.transport.quic", err=%e, "failed to accept packet connection");
            }
          }
        }
      }
    }
  }

  async fn handle_packet(
    mut recv_stream: S::ReadStream,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "compression")] offload_size: usize,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut tag = [0u8; 2];
    let mut readed = 0;
    recv_stream
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    let mut packet_label = if tag[0] == Label::TAG {
      let label_size = tag[1] as usize;
      // consume peeked
      recv_stream.read_exact(&mut tag).await.unwrap();

      let mut label = vec![0u8; label_size];
      recv_stream
        .read_exact(&mut label)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += 2 + label_size;
      Label::try_from(Bytes::from(label)).map_err(|e| QuicTransportError::Label(e.into()))?
    } else {
      Label::empty()
    };

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target: "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    #[cfg(not(feature = "compression"))]
    return {
      let (read, msgs) = Self::decode_without_compression(&mut recv_stream).await?;
      readed += read;
      if let Err(e) = recv_stream.close().await {
        tracing::error!(target: "memberlist.transport.quic", err = %e, "failed to close remote packet connection");
      }
      Ok((readed, msgs))
    };

    #[cfg(feature = "compression")]
    {
      let (read, msgs) = Self::decode_with_compression(&mut recv_stream, offload_size).await?;
      readed += read;
      if let Err(e) = recv_stream.close().await {
        tracing::error!(target: "memberlist.transport.quic", err = %e, "failed to close remote packet connection");
      }
      Ok((readed, msgs))
    }
  }

  fn decode_batch(
    mut src: &[u8],
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let num_msgs = src[0] as usize;
    src = &src[1..];
    let mut msgs = OneOrMore::with_capacity(num_msgs);

    for _ in 0..num_msgs {
      let expected_msg_len = NetworkEndian::read_u32(&src[..MAX_MESSAGE_LEN_SIZE]) as usize;
      src = &src[MAX_MESSAGE_LEN_SIZE..];
      let (readed, msg) =
        <T::Wire as Wire>::decode_message(src).map_err(QuicTransportError::Wire)?;

      debug_assert_eq!(
        expected_msg_len, readed,
        "expected message length {expected_msg_len} but got {readed}",
      );
      src = &src[readed..];
      msgs.push(msg);
    }

    Ok(msgs)
  }

  async fn decode_without_compression(
    conn: &mut S::ReadStream,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut read = 0;
    let mut tag = [0u8; HEADER_SIZE];
    conn
      .read_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    read += HEADER_SIZE;

    if Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG == tag[0] {
      let msg_len = NetworkEndian::read_u32(&tag[1..]) as usize;

      if msg_len < MAX_INLINED_BYTES {
        let mut buf = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact(&mut buf[..msg_len + 1])
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len + 1;
        Self::decode_batch(&buf[..msg_len + 1]).map(|msgs| (read, msgs))
      } else {
        let mut buf = vec![0; msg_len + 1];
        conn
          .read_exact(&mut buf)
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len + 1;
        Self::decode_batch(&buf).map(|msgs| (read, msgs))
      }
    } else {
      let msg_len = NetworkEndian::read_u32(&tag[1..]) as usize;
      if msg_len <= MAX_INLINED_BYTES {
        let mut buf = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact(&mut buf[..msg_len])
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len;
        <T::Wire as Wire>::decode_message(&buf[..msg_len])
          .map(|(_, msg)| (read, msg.into()))
          .map_err(QuicTransportError::Wire)
      } else {
        let mut buf = vec![0; msg_len];
        conn
          .read_exact(&mut buf)
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len;
        <T::Wire as Wire>::decode_message(&buf)
          .map(|(_, msg)| (read, msg.into()))
          .map_err(QuicTransportError::Wire)
      }
    }
  }

  #[cfg(feature = "compression")]
  async fn decode_with_compression(
    conn: &mut S::ReadStream,
    offload_size: usize,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut tag = [0u8; HEADER_SIZE];
    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    if !COMPRESS_TAG.contains(&tag[0]) {
      return Self::decode_without_compression(conn).await;
    }

    // consume peeked header
    conn.read_exact(&mut tag).await.unwrap();
    let readed = HEADER_SIZE;
    let compressor = Compressor::try_from(tag[0])?;
    let msg_len = NetworkEndian::read_u32(&tag[1..]) as usize;

    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      conn
        .read_exact(&mut buf[..msg_len])
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let compressed = &buf[..msg_len];
      Self::decompress_and_decode(compressor, compressed).map(|msgs| (readed + msg_len, msgs))
    } else if msg_len <= offload_size {
      let mut buf = vec![0; msg_len];
      conn
        .read_exact(&mut buf)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let compressed = &buf[..msg_len];
      Self::decompress_and_decode(compressor, compressed).map(|msgs| (readed + msg_len, msgs))
    } else {
      let mut buf = vec![0; msg_len];
      conn
        .read_exact(&mut buf)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let (tx, rx) = futures::channel::oneshot::channel();
      rayon::spawn(move || {
        if tx
          .send(Self::decompress_and_decode(compressor, &buf))
          .is_err()
        {
          tracing::error!(target: "memberlist.transport.quic", "failed to send decompressed message");
        }
      });

      match rx.await {
        Ok(Ok(msgs)) => Ok((readed + msg_len, msgs)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(QuicTransportError::ComputationTaskFailed),
      }
    }
  }

  #[cfg(feature = "compression")]
  fn decompress_and_decode(
    compressor: Compressor,
    src: &[u8],
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    use bytes::Buf;

    let mut uncompressed: Bytes = compressor.decompress(src)?.into();
    let tag = uncompressed[0];
    let msg_len = NetworkEndian::read_u32(&uncompressed[1..]) as usize;
    uncompressed.advance(HEADER_SIZE);

    if uncompressed.remaining() < msg_len + 1 {
      return Err(QuicTransportError::Custom(
        "uncompressed data do not have enough bytes to decode".into(),
      ));
    }

    if Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG == tag {
      let num_msgs = uncompressed[0] as usize;
      let mut msgs = OneOrMore::with_capacity(num_msgs);
      uncompressed.advance(1);
      for _ in 0..num_msgs {
        let expected_msg_len =
          NetworkEndian::read_u32(&uncompressed[..MAX_MESSAGE_LEN_SIZE]) as usize;
        uncompressed.advance(MAX_MESSAGE_LEN_SIZE);
        let (readed, msg) =
          <T::Wire as Wire>::decode_message(&uncompressed).map_err(QuicTransportError::Wire)?;
        debug_assert_eq!(
          expected_msg_len, readed,
          "expected bytes read {expected_msg_len} but got {readed}",
        );
        uncompressed.advance(readed);
        msgs.push(msg);
      }

      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(&uncompressed)
        .map(|(_, msg)| msg.into())
        .map_err(QuicTransportError::Wire)
    }
  }
}
