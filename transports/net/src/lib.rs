use std::{
  io::{self, Error, ErrorKind},
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use checksum::CHECKSUM_SIZE;
use compressor::UnknownCompressor;
use futures::{io::BufReader, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, FutureExt};
use label::{LabelAsyncIOExt, LabelBufExt};
use security::{Encryptor, SecurityError};
use showbiz_core::{
  transport::{
    resolver::AddressResolver,
    stream::{
      packet_stream, promised_stream, PacketProducer, PacketSubscriber, StreamProducer,
      StreamSubscriber,
    },
    Id, Transport, TransportError, Wire,
  },
  types::{Message, Packet, SmallVec, TinyVec},
  CheapClone,
};
use showbiz_utils::{net::CIDRsPolicy, OneOrMore};
use wg::AsyncWaitGroup;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;
#[cfg(feature = "compression")]
use compressor::Compressor;

/// Encrypt/decrypt related.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod security;
#[cfg(feature = "encryption")]
use security::{EncryptionAlgo, SecretKey, SecretKeyring, SecretKeys};

/// Errors for the net transport.
pub mod error;
use error::*;

/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`NetTransport`](crate::NetTransport).
pub mod stream_layer;
use stream_layer::*;

mod label;
pub use label::Label;

mod checksum;
pub use checksum::Checksumer;

use crate::label::{LabelBufMutExt, LabelError};

const CHECKSUM_TAG: core::ops::RangeInclusive<u8> = 44..=64;
const ENCRYPT_TAG: core::ops::RangeInclusive<u8> = 65..=85;
const COMPRESS_TAG: core::ops::RangeInclusive<u8> = 86..=126;
const LABEL_TAG: u8 = 127;

const MAX_PACKET_SIZE: usize = u16::MAX as usize;
/// max message bytes is `u16::MAX`
const PACKET_OVERHEAD: usize = core::mem::size_of::<u16>();
/// tag + num msgs (max number of messages is `255`)
const PACKET_HEADER_OVERHEAD: usize = 1 + 1;
const NUM_PACKETS_PER_BATCH: usize = 255;
const MAX_INLINED_BYTES: usize = 64;

#[cfg(feature = "compression")]
const PROMISED_COMPRESS_OVERHEAD: usize = 1 + core::mem::size_of::<u32>();

const DEFAULT_PORT: u16 = 7946;
const LABEL_MAX_SIZE: usize = 255;
const DEFAULT_BUFFER_SIZE: usize = 4096;
const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;

/// Used to buffer incoming packets during read
/// operations.
const PACKET_BUF_SIZE: usize = 65536;

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

/// Errors that can occur when using [`NetTransport`].
#[derive(thiserror::Error)]
pub enum NetTransportError<A: AddressResolver, W: Wire> {
  /// Connection error.
  #[error("connection error: {0}")]
  Connection(#[from] ConnectionError),
  /// IO error.
  #[error("io error: {0}")]
  IO(#[from] std::io::Error),
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no interface addresses found.
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddrs,
  /// Returns when the ip is blocked.
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
  /// Returns when the packet buffer size is too small.
  #[error("failed to resize packet buffer {0}")]
  ResizePacketBuffer(std::io::Error),
  /// Returns when the packet socket fails to bind.
  #[error("failed to start packet listener on {0}: {1}")]
  ListenPacket(SocketAddr, std::io::Error),
  /// Returns when the promised listener fails to bind.
  #[error("failed to start promised listener on {0}: {1}")]
  ListenPromised(SocketAddr, std::io::Error),
  /// Returns when we fail to resolve an address.
  #[error("failed to resolve address {addr}: {err}")]
  Resolve { addr: A::Address, err: A::Error },
  /// Returns when fail to compress
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error("failed to compress message: {0}")]
  Compress(#[from] compressor::CompressError),
  /// Returns when fail to decompress
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error("failed to decompress message: {0}")]
  Decompress(#[from] compressor::DecompressError),
  /// Returns when there is a unkstartn compressor.
  #[cfg(feature = "compression")]
  #[error("{0}")]
  UnkstartnCompressor(#[from] UnknownCompressor),
  /// Returns when there is a security error. e.g. encryption/decryption error.
  #[error("{0}")]
  Security(#[from] SecurityError),
  /// Returns when receiving a compressed message, but compression is disabled.
  #[error("receive a compressed message, but compression is disabled on this node")]
  CompressionDisabled,
  /// Returns when the label error.
  #[error("{0}")]
  Label(#[from] label::LabelError),
  /// Returns when getting unkstartn checksumer
  #[error("{0}")]
  UnkstartnChecksumer(#[from] checksum::UnknownChecksumer),
  /// Returns when the computation task panic
  #[error("computation task panic")]
  ComputationTaskFailed,
  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),
}

impl<A: AddressResolver, W: Wire> core::fmt::Debug for NetTransportError<A, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A: AddressResolver, W: Wire> TransportError for NetTransportError<A, W> {
  fn io(err: std::io::Error) -> Self {
    Self::IO(err)
  }

  fn is_remote_failure(&self) -> bool {
    if let Self::Connection(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn is_unexpected_eof(&self) -> bool {
    match self {
      Self::Connection(e) => e.is_eof(),
      Self::IO(e) => e.kind() == std::io::ErrorKind::UnexpectedEof,
      _ => false,
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}

fn default_gossip_verify_outgoing() -> bool {
  true
}

/// Used to configure a net transport.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize",
    deserialize = "I: for<'a> serde::Deserialize<'a>, A: AddressResolver, A::Address: for<'a> serde::Deserialize<'a>, A::ResolvedAddress: for<'a> serde::Deserialize<'a>"
  ))
)]
pub struct NetTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  /// The local node's ID.
  id: I,

  /// The local node's address.
  address: A::Address,

  /// The address to advertise to other nodes. If not set,
  /// the transport will attempt to discover the local IP address
  /// to use.
  advertise_address: Option<A::ResolvedAddress>,

  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(getter(style = "ref", const,))]
  bind_addrs: SmallVec<IpAddr>,
  bind_port: Option<u16>,

  label: Label,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  skip_inbound_label_check: bool,

  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[cfg_attr(feature = "serde", serde(default = "default_gossip_verify_outgoing"))]
  gossip_verify_outgoing: bool,

  max_payload_size: usize,

  /// Policy for Classless Inter-Domain Routing (CIDR).
  ///
  /// By default, allow any connection
  #[viewit(getter(style = "ref", const,))]
  #[cfg_attr(feature = "serde", serde(default))]
  cidrs_policy: CIDRsPolicy,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  compressor: Option<Compressor>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  #[cfg(any(feature = "compression", feature = "encryption"))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "compression", feature = "encryption"))))]
  offload_size: usize,

  /// Used to initialize the primary encryption key in a keyring.
  ///
  /// **Note: This field will not be used when network layer is secure**
  ///
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  primary_key: Option<SecretKey>,

  /// Holds all of the encryption keys used internally. It is
  /// automatically initialized using the SecretKey and SecretKeys values.
  ///
  /// **Note: This field will not be used if the network layer is secure.**
  #[viewit(getter(
    style = "ref",
    result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeys>")
  ))]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  secret_keys: Option<SecretKeys>,

  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  encryption_algo: Option<EncryptionAlgo>,

  /// The checksumer to use for checksumming packets.
  #[cfg_attr(feature = "serde", serde(default))]
  checksumer: Checksumer,

  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<showbiz_utils::MetricLabels>>,
}

pub struct NetTransport<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer, W> {
  opts: Arc<NetTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  promised_listeners: SmallVec<Arc<S::Listener>>,
  sockets: SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>,
  stream_layer: Arc<S>,
  #[cfg(feature = "encryption")]
  encryptor: Option<Encryptor>,

  wg: AsyncWaitGroup,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<W>,
}

impl<I, A, S, W> NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  /// Creates a new net transport.
  pub async fn new(
    resolver: A,
    stream_layer: S,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A, W>> {
    match opts.bind_port {
      Some(0) | None => Self::retry(resolver, stream_layer, 10, opts).await,
      _ => Self::retry(resolver, stream_layer, 1, opts).await,
    }
  }

  async fn new_in(
    resolver: Arc<A>,
    stream_layer: Arc<S>,
    opts: Arc<NetTransportOptions<I, A>>,
    #[cfg(feature = "encryption")] encryptor: Option<Encryptor>,
  ) -> Result<Self, NetTransportError<A, W>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addrs.is_empty() {
      return Err(NetTransportError::EmptyBindAddrs);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut promised_listeners = Vec::with_capacity(opts.bind_addrs.len());
    let mut sockets = Vec::with_capacity(opts.bind_addrs.len());
    let bind_port = opts.bind_port.unwrap_or(0);
    for &addr in opts.bind_addrs.iter() {
      let addr = SocketAddr::new(addr, bind_port);
      let (local_addr, ln) = match stream_layer.bind(addr).await {
        Ok(ln) => (ln.local_addr().unwrap(), ln),
        Err(e) => return Err(NetTransportError::ListenPromised(addr, e)),
      };
      promised_listeners.push((Arc::new(ln), local_addr));
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };

      let (local_addr, packet_socket) =
        <<<A::Runtime as Runtime>::Net as Net>::UdpSocket as UdpSocket>::bind(addr)
          .await
          .map(|ln| (addr, ln))
          .map_err(|e| NetTransportError::ListenPacket(addr, e))?;
      sockets.push((Arc::new(packet_socket), local_addr));
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for ((promised_ln, promised_addr), (socket, socket_addr)) in
      promised_listeners.iter().zip(sockets.iter())
    {
      wg.add(2);
      PromisedProcessor::<A, Self, S> {
        wg: wg.clone(),
        stream_tx: stream_tx.clone(),
        ln: promised_ln.clone(),
        shutdown: shutdown.clone(),
        shutdown_rx: shutdown_rx.clone(),
        local_addr: *promised_addr,
      }
      .run();

      PacketProcessor::<A, Self> {
        wg: wg.clone(),
        packet_tx: packet_tx.clone(),
        label: opts.label.clone(),
        offload_size: opts.offload_size,
        #[cfg(feature = "encryption")]
        encryptor: encryptor.clone(),
        socket: socket.clone(),
        local_addr: *socket_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
        shutdown_rx: shutdown_rx.clone(),
        skip_inbound_label_check: opts.skip_inbound_label_check,
      }
      .run();
    }

    // find final advertise address
    let advertise_addr = match opts.advertise_address {
      Some(addr) => addr,
      None => {
        let addr = if opts.bind_addrs[0].is_unspecified() {
          local_ip_address::local_ip().map_err(|e| match e {
            local_ip_address::Error::LocalIpAddressNotFound => NetTransportError::NoPrivateIP,
            e => NetTransportError::NoInterfaceAddresses(e),
          })?
        } else {
          promised_listeners[0].1.ip()
        };

        // Use the port we are bound to.
        SocketAddr::new(addr, promised_listeners[0].1.port())
      }
    };

    Ok(Self {
      advertise_addr,
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      promised_listeners: promised_listeners.into_iter().map(|(ln, _)| ln).collect(),
      sockets: sockets.into_iter().map(|(ln, _)| ln).collect(),
      stream_layer,
      #[cfg(feature = "encryption")]
      encryptor,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  async fn retry(
    resolver: A,
    stream_layer: S,
    limit: usize,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A, W>> {
    let mut i = 0;
    let resolver = Arc::new(resolver);
    let stream_layer = Arc::new(stream_layer);
    let opts = Arc::new(opts);
    #[cfg(feature = "encryption")]
    let keyring = if S::is_secure() || opts.encryption_algo.is_none() {
      None
    } else {
      match (opts.primary_key, &opts.secret_keys) {
        (None, Some(keys)) if !keys.is_empty() => {
          tracing::warn!(target: "showbiz", "using first key in keyring as primary key");
          let mut iter = keys.iter().copied();
          let pk = iter.next().unwrap();
          let keyring = SecretKeyring::with_keys(pk, iter);
          Some(keyring)
        }
        (Some(pk), None) => Some(SecretKeyring::new(pk)),
        (Some(pk), Some(keys)) => Some(SecretKeyring::with_keys(pk, keys.iter().copied())),
        _ => None,
      }
    }
    .map(|kr| Encryptor::new(opts.encryption_algo.unwrap(), kr));
    loop {
      #[cfg(feature = "metrics")]
      let transport = {
        Self::new_in(
          resolver.clone(),
          stream_layer.clone(),
          opts.clone(),
          #[cfg(feature = "encryption")]
          keyring.clone(),
        )
        .await
      };

      match transport {
        Ok(t) => {
          if let Some(0) | None = opts.bind_port {
            let port = t.advertise_addr.port();
            tracing::warn!(target:  "showbiz", "using dynamic bind port {port}");
          }
          return Ok(t);
        }
        Err(e) => {
          tracing::debug!(target="showbiz", err=%e, "fail to create transport");
          if i == limit - 1 {
            return Err(e);
          }
          i += 1;
        }
      }
    }
  }

  fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();
    overhead += 1 + CHECKSUM_SIZE;

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1;
    }

    #[cfg(feature = "encryption")]
    if let Some(encryptor) = &self.encryptor {
      overhead += encryptor.encrypt_overhead();
    }

    overhead
  }

  async fn send_batch(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
    start: Instant,
  ) -> Result<usize, NetTransportError<A, W>> {
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len);
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    #[cfg(feature = "encryption")]
    let nonce =
      if let (Some(enp), true) = (self.encryptor.as_ref(), self.opts.gossip_verify_outgoing) {
        let nonce = enp.write_header(&mut buf);
        Some(nonce)
      } else {
        None
      };

    let checksum_offset = buf.len();
    let mut offset = buf.len();

    // reserve to store checksum
    buf.put_u8(self.opts.checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(batch.estimate_encoded_len, 0);

    // Encode compound message header
    buf[offset] = Message::<I, A::ResolvedAddress>::COMPOUND_TAG;
    offset += 1;
    buf[offset] = batch.num_packets as u8;
    offset += 1;

    // Encode messages to buffer
    for packet in batch.packets {
      let expected_packet_encoded_size = W::encoded_len(&packet);
      NetworkEndian::write_u16(
        &mut buf[offset..offset + PACKET_OVERHEAD],
        expected_packet_encoded_size as u16,
      );
      offset += PACKET_OVERHEAD;
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(NetTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} (calculated by Wire::encoded_len()) is not match the actual encoded size {} returned by Wire::encode_message()",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    self
      .send_to(
        addr,
        buf,
        checksum_offset,
        offset - (checksum_offset + 1 + CHECKSUM_SIZE),
        batch.estimate_encoded_len,
        nonce,
        start,
      )
      .await
      .map(|(sent, _)| sent)
  }

  #[allow(clippy::too_many_arguments)]
  async fn send_to(
    &self,
    addr: &A::ResolvedAddress,
    mut buf: BytesMut,
    checksum_offset: usize,
    data_size: usize,
    total_bytes: usize,
    nonce: Option<[u8; security::NONCE_SIZE]>,
    start: Instant,
  ) -> Result<(usize, Instant), NetTransportError<A, W>> {
    #[cfg(feature = "compression")]
    if let Some(compressor) = self.opts.compressor {
      if data_size > self.opts.offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut messages_bytes = buf.split_off(checksum_offset);
        let encryptor = if nonce.is_some() {
          let enp = self.encryptor.as_ref().unwrap();
          let pk = enp.kr.primary_key().await;
          Some((pk, enp.clone()))
        } else {
          None
        };
        let label = self.opts.label.cheap_clone();
        let checksumer = self.opts.checksumer;
        let max_payload_size = self.max_payload_size();
        rayon::spawn(move || {
          let data_offset = checksum_offset + 1 + CHECKSUM_SIZE;
          let compressed = match compressor
            .compress_to_bytes(&messages_bytes[data_offset..])
            .map_err(NetTransportError::Compress)
          {
            Ok(compressed) => compressed,
            Err(e) => {
              if tx.send(Err(e)).is_err() {
                tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
              }
              return;
            }
          };

          messages_bytes[data_offset..data_offset + compressed.len()].copy_from_slice(&compressed);
          messages_bytes.truncate(data_offset + compressed.len());

          // check if the packet exceeds the max packet size can be sent by the packet layer
          let packet_size = data_offset + compressed.len();
          if packet_size >= max_payload_size {
            if tx
              .send(Err(NetTransportError::PacketTooLarge(packet_size)))
              .is_err()
            {
              tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
            }
            return;
          }

          let cks = checksumer.checksum(&messages_bytes[data_offset..]);
          NetworkEndian::write_u32(&mut messages_bytes[1..1 + CHECKSUM_SIZE], cks);

          #[cfg(feature = "encryption")]
          if let Some((pk, enp)) = encryptor {
            let nonce = nonce.unwrap();
            return {
              if tx
                .send(
                  enp
                    .encrypt(pk, nonce, label.as_bytes(), &mut messages_bytes)
                    .map(|_| messages_bytes)
                    .map_err(Into::into),
                )
                .is_err()
              {
                tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
              }
            };
          }

          if tx.send(Ok(messages_bytes)).is_err() {
            tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
          }
        });

        match rx.await {
          Ok(Ok(messages_bytes)) => {
            buf.unsplit(messages_bytes);
          }
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(NetTransportError::ComputationTaskFailed),
        }
      } else {
        let offset = checksum_offset + 1 + CHECKSUM_SIZE;
        let compressed = compressor
          .compress_to_bytes(&buf[offset..offset + data_size])
          .map_err(NetTransportError::Compress)?;
        buf[offset..offset + compressed.len()].copy_from_slice(&compressed);
        buf.truncate(offset + compressed.len());

        // check if the packet exceeds the max packet size can be sent by the packet layer
        if buf.len() >= self.max_payload_size() {
          return Err(NetTransportError::PacketTooLarge(total_bytes));
        }

        let cks = self.opts.checksumer.checksum(&buf[offset..]);
        NetworkEndian::write_u32(
          &mut buf[checksum_offset + 1..checksum_offset + 1 + CHECKSUM_SIZE],
          cks,
        );

        #[cfg(feature = "encryption")]
        if let (Some(enp), true) = (&self.encryptor, self.opts.gossip_verify_outgoing) {
          let nonce = nonce.unwrap();
          let pk = enp.kr.primary_key().await;
          let mut dst = buf.split_off(checksum_offset);
          enp
            .encrypt(pk, nonce, self.opts.label.as_bytes(), &mut dst)
            .map(|_| {
              buf.unsplit(dst);
            })?;
        }
      }

      return self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map(|sent| (sent, start))
        .map_err(|e| {
          NetTransportError::Connection(ConnectionError {
            kind: ConnectionKind::Packet,
            error_kind: ConnectionErrorKind::Write,
            error: e,
          })
        });
    }

    #[cfg(feature = "encryption")]
    if let (Some(enp), true) = (&self.encryptor, self.opts.gossip_verify_outgoing) {
      let nonce = nonce.unwrap();
      let mut dst = buf.split_off(checksum_offset);
      let pk = enp.kr.primary_key().await;
      enp
        .encrypt(pk, nonce, self.opts.label.as_bytes(), &mut dst)
        .map(|_| {
          buf.unsplit(dst);
        })?;
    }

    self.sockets[0]
      .send_to(&buf, addr)
      .await
      .map(|sent| (sent, start))
      .map_err(|e| {
        NetTransportError::Connection(ConnectionError {
          kind: ConnectionKind::Packet,
          error_kind: ConnectionErrorKind::Write,
          error: e,
        })
      })
  }

  fn should_offload_for_sending(&self, size: usize) -> bool {
    #[cfg(feature = "compression")]
    {
      let compression_enabled = self.opts.compressor.is_some();
      if size >= self.opts.offload_size && compression_enabled {
        return true;
      }
    }

    #[cfg(feature = "encryption")]
    {
      let encryption_enabled = self.encryptor.is_some();
      if size >= self.opts.offload_size && encryption_enabled && !S::is_secure() {
        return true;
      }
    }

    false
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  fn compress_and_encrypt(
    compressor: &Compressor,
    encryptor: &Encryptor,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let encrypt_header = encryptor.encrypt_overhead();
    let total_len = encrypt_header + PROMISED_COMPRESS_OVERHEAD + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    let nonce = encryptor.write_header(&mut buf);
    buf.resize(total_len, 0);

    W::encode_message(msg, &mut buf[encrypt_header..]).map_err(NetTransportError::Wire)?;

    let compressed = compressor
      .compress_to_bytes(&buf[encrypt_header..])
      .map_err(NetTransportError::Compress)?;
    let compressed_size = compressed.len();
    buf.truncate(encrypt_header);
    buf.put_u8(*compressor as u8);
    let mut compress_size_buf = [0; core::mem::size_of::<u32>()];
    NetworkEndian::write_u32(&mut compress_size_buf, compressed_size as u32);
    buf.put_slice(&compress_size_buf);
    buf.put_slice(&compressed);

    let mut dst = buf.split_off(encrypt_header);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "encryption")]
  fn encrypt(
    encryptor: &Encryptor,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let encrypt_header = encryptor.encrypt_overhead();
    let total_len = encrypt_header + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    let nonce = encryptor.write_header(&mut buf);
    buf.resize(total_len, 0);

    W::encode_message(msg, &mut buf[encrypt_header..]).map_err(NetTransportError::Wire)?;

    let mut dst = buf.split_off(encrypt_header);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "encryption")]
  async fn send_message_with_encryption_without_compression(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
    stream_label: &Label,
  ) -> Result<usize, NetTransportError<A, W>> {
    let enp = match self.encryptor.as_ref() {
      Some(enp) if self.opts.gossip_verify_outgoing => enp,
      _ => {
        return self
          .send_message_without_compression_and_encryption(conn, target, msg)
          .await
      }
    };

    let encoded_size = W::encoded_len(&msg);
    let buf = if encoded_size < self.opts.offload_size {
      let pk = enp.kr.primary_key().await;
      Self::encrypt(enp, pk, &self.opts.label, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let pk = enp.kr.primary_key().await;
      let stream_label = stream_label.cheap_clone();
      let enp = enp.clone();
      rayon::spawn(move || {
        if tx
          .send(Self::encrypt(&enp, pk, &stream_label, msg, encoded_size))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  #[cfg(feature = "compression")]
  fn compress(
    compressor: Compressor,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<Vec<u8>, NetTransportError<A, W>> {
    let mut buf = Vec::with_capacity(PROMISED_COMPRESS_OVERHEAD + encoded_size);
    buf.push(compressor as u8);
    buf.extend(&[0; 4]);

    let data = W::encode_message_to_vec(msg).map_err(NetTransportError::Wire)?;
    compressor
      .compress_to_vec(&data, &mut buf)
      .map_err(NetTransportError::Compress)?;

    let data_len = buf.len() - PROMISED_COMPRESS_OVERHEAD;
    NetworkEndian::write_u32(&mut buf[1..PROMISED_COMPRESS_OVERHEAD], data_len as u32);

    Ok(buf)
  }

  #[cfg(feature = "compression")]
  async fn send_message_with_compression_without_encryption(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let compressor = match self.opts.compressor {
      Some(c) => c,
      None => {
        return self
          .send_message_without_compression_and_encryption(conn, target, msg)
          .await
      }
    };

    let encoded_size = W::encoded_len(&msg);

    let buf = if encoded_size < self.opts.offload_size {
      Self::compress(compressor, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      rayon::spawn(move || {
        if tx
          .send(Self::compress(compressor, msg, encoded_size))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;
    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  async fn send_message_without_compression_and_encryption(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let data = W::encode_message_to_bytes(msg).map_err(NetTransportError::Wire)?;
    let total_data = data.len();
    conn
      .write_all(&data)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_data)
  }
}

struct Batch<I, A> {
  num_packets: usize,
  packets: TinyVec<Message<I, A>>,
  estimate_encoded_len: usize,
}

impl<I, A, S, W> Transport for NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  type Error = NetTransportError<Self::Resolver, Self::Wire>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

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

  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.opts.address
  }

  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  fn max_payload_size(&self) -> usize {
    MAX_PACKET_SIZE.min(self.opts.max_payload_size)
  }

  fn packet_overhead(&self) -> usize {
    PACKET_OVERHEAD
  }

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
    from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    let mut conn = BufReader::new(conn);
    let mut tag = [0; 1];
    conn.read_exact(&mut tag).await.map_err(|e| {
      tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive");
      Self::Error::Connection(ConnectionError::promised_read(e))
    })?;

    let mut stream_label = if tag[0] == LABEL_TAG {
      let mut buf = [0u8; 1];
      conn.read_exact(&mut buf).await.map_err(|e| {
        tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive and remove the stream label header");
        Self::Error::Connection(ConnectionError::promised_read(e))
      })?;

      let len = buf[0] as usize;
      let mut buf = vec![0u8; len];
      conn.read_exact(&mut buf).await.map_err(|e| {
        tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive and remove the stream label header");

        Self::Error::Connection(ConnectionError::promised_read(e))
      })?;

      let buf: Bytes = buf.into();
      Label::try_from(buf)
        .map_err(|e| {
          tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive and remove the stream label header");

          Self::Error::IO(io::Error::new(io::ErrorKind::InvalidData, e))
        })?
    } else {
      Label::empty()
    };

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target: "showbiz.net.promised", "unexpected double stream label header");
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(target: "showbiz.net.promised", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), stream_label).into());
    }

    let tag = if tag[0] == LABEL_TAG {
      conn.read_exact(&mut tag)
        .await
        .map_err(|e| {
          tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive");
          Self::Error::Connection(ConnectionError::promised_read(e))
        })?;
      tag[0]
    } else {
      tag[0]
    };

    // Check if the message is encrypted
    if ENCRYPT_TAG.contains(&tag) {
      #[cfg(not(feature = "encryption"))]
      {
        tracing::error!(target: "showbiz.net.promised", remote = %from, "received encrypted message, but encryption is disabled");
        return Err(Self::Error::Security(SecurityError::Disabled));
      }

      if self.encryptor.is_none() {
        tracing::error!(target: "showbiz.net.promised", remote = %from, "received encrypted message, but encryption is disabled");
        return Err(Self::Error::Security(SecurityError::Disabled));
      }

      todo!()
    }

    todo!()
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    target: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return self
      .send_message_without_compression_and_encryption(conn, target, msg)
      .await;

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return self
      .send_message_with_compression_without_encryption(conn, target, msg)
      .await;

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return self
      .send_message_with_encryption_without_compression(conn, target, msg, &self.opts.label)
      .await;

    let compression_enabled = self.opts.compressor.is_some();
    let encryption_enabled = self.encryptor.is_some() && self.opts.gossip_verify_outgoing;

    if !compression_enabled && !encryption_enabled {
      return self
        .send_message_without_compression_and_encryption(conn, target, msg)
        .await;
    }

    if compression_enabled && !encryption_enabled {
      return self
        .send_message_with_compression_without_encryption(conn, target, msg)
        .await;
    }

    if !compression_enabled && encryption_enabled {
      return self
        .send_message_with_encryption_without_compression(conn, target, msg, &self.opts.label)
        .await;
    }

    let encoded_size = W::encoded_len(&msg);
    let encryptor = self.encryptor.as_ref().unwrap();
    let compressor = self.opts.compressor.unwrap();

    let buf = if encoded_size < self.opts.offload_size {
      let pk = encryptor.kr.primary_key().await;
      Self::compress_and_encrypt(
        &compressor,
        encryptor,
        pk,
        &self.opts.label,
        msg,
        encoded_size,
      )?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let encryptor = encryptor.clone();
      let pk = encryptor.kr.primary_key().await;
      let stream_label = self.opts.label.cheap_clone();

      rayon::spawn(move || {
        if tx
          .send(Self::compress_and_encrypt(
            &compressor,
            &encryptor,
            pk,
            &stream_label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();
    let packet_encoded_len = W::encoded_len(&packet);
    let encoded_len = self.fix_packet_overhead() + packet_encoded_len;

    // If compression is not enabled, we can check the packet size first.
    #[cfg(feature = "compression")]
    if self.opts.compressor.is_none() && encoded_len >= self.max_payload_size() {
      return Err(Self::Error::PacketTooLarge(encoded_len));
    }

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(encoded_len);
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();
    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    #[cfg(feature = "encryption")]
    let nonce =
      if let (Some(enp), true) = (self.encryptor.as_ref(), self.opts.gossip_verify_outgoing) {
        let nonce = enp.write_header(&mut buf);
        Some(nonce)
      } else {
        None
      };

    let checksum_offset = buf.len();
    let mut offset = buf.len();

    // reserve to store checksum
    buf.put_u8(self.opts.checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(offset + packet_encoded_len, 0);

    W::encode_message(packet, &mut buf[offset..offset + packet_encoded_len])
      .map_err(Self::Error::Wire)?;

    self
      .send_to(
        addr,
        buf,
        checksum_offset,
        packet_encoded_len,
        encoded_len,
        nonce,
        start,
      )
      .await
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();

    let mut batches =
      SmallVec::<Batch<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>::new();
    let packets_overhead = self.packets_header_overhead();
    let mut estimate_batch_encoded_size = 0;
    let mut current_packets_in_batch = 0;

    // get how many packets a batch
    for (idx, packet) in packets.iter().enumerate() {
      let ep_len = W::encoded_len(packet);
      // check if we reach the maximum packet size
      let current_encoded_size =
        ep_len + packets_overhead + idx * PACKET_OVERHEAD + estimate_batch_encoded_size;
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
          addr,
          Batch {
            num_packets: packets.len(),
            packets,
            estimate_encoded_len: estimate_batch_encoded_size,
          },
          start,
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
        futures::future::join_all(batches.into_iter().map(|b| self.send_batch(addr, b, start)))
          .await;

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
    timeout: Duration,
  ) -> Result<Self::Stream, Self::Error> {
    let connector = <Self::Runtime as Runtime>::timeout(timeout, self.stream_layer.connect(*addr));
    match connector.await {
      Ok(Ok(mut conn)) => {
        if self.opts.label.is_empty() {
          Ok(conn)
        } else {
          conn
            .add_label_header(&self.opts.label)
            .await
            .map_err(|e| Self::Error::Connection(ConnectionError::promised_write(e)))?;
          Ok(conn)
        }
      }
      Ok(Err(e)) => Err(Self::Error::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: e,
      })),
      Err(_) => Err(NetTransportError::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: Error::new(ErrorKind::TimedOut, "timeout"),
      })),
    }
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
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }

  fn block_shutdown(&self) -> Result<(), Self::Error> {
    use pollster::FutureExt as _;
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();
    let wg = self.wg.clone();

    wg.wait().block_on();
    Ok(())
  }
}

struct PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  wg: AsyncWaitGroup,
  stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, S::Stream>,
  ln: Arc<S::Listener>,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<A, T, S> PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  fn run(self) {
    let Self {
      wg,
      stream_tx,
      ln,
      shutdown,
      local_addr,
      ..
    } = self;

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      let mut loop_delay = Duration::ZERO;
      loop {
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = ln.accept().fuse() => {
            match rst {
              Ok((conn, remote_addr)) => {
                // No error, reset loop delay
                loop_delay = Duration::ZERO;
                if let Err(e) = stream_tx
                  .send(remote_addr, conn)
                  .await
                {
                  tracing::error!(target:  "showbiz.net.transport", local_addr=%local_addr, err = %e, "failed to send TCP connection");
                }
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                if loop_delay == Duration::ZERO {
                  loop_delay = BASE_DELAY;
                } else {
                  loop_delay *= 2;
                }

                if loop_delay > MAX_DELAY {
                  loop_delay = MAX_DELAY;
                }

                tracing::error!(target:  "showbiz.net.transport", local_addr=%local_addr, err = %e, "error accepting TCP connection");
                <T::Runtime as Runtime>::sleep(loop_delay).await;
                continue;
              }
            }
          }
        }
      }
    });
  }
}

struct PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  wg: AsyncWaitGroup,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  socket: Arc<<<T::Runtime as Runtime>::Net as Net>::UdpSocket>,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  label: Label,
  #[cfg(feature = "encryption")]
  encryptor: Option<Encryptor>,
  offload_size: usize,
  skip_inbound_label_check: bool,
  #[cfg(feature = "metrics")]
  metric_labels: Arc<showbiz_utils::MetricLabels>,
}

impl<A, T> PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  fn run(self) {
    let Self {
      wg,
      packet_tx,
      socket,
      shutdown,
      local_addr,
      ..
    } = self;

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      tracing::info!(
        target: "showbiz.net.transport",
        "udp listening on {local_addr}"
      );

      loop {
        // Do a blocking read into a fresh buffer. Grab a time stamp as
        // close as possible to the I/O.
        let mut buf = BytesMut::new();
        buf.resize(PACKET_BUF_SIZE, 0);
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = socket.recv_from(&mut buf).fuse() => {
            match rst {
              Ok((n, addr)) => {
                // Check the length - it needs to have at least one byte to be a
                // proper message.
                if n < 1 {
                  tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = "UDP packet too short (0 bytes)");
                  continue;
                }
                buf.truncate(n);
                let start = Instant::now();
                let msg = match Self::handle_remote_bytes(
                  buf,
                  &self.label,
                  self.skip_inbound_label_check,
                  #[cfg(feature = "encryption")]
                  self.encryptor.as_ref(),
                  #[cfg(any(feature = "compression", feature = "encryption"))]
                  self.offload_size,
                ).await {
                  Ok(msg) => msg,
                  Err(e) => {
                    tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = %e, "fail to handle UDP packet");
                    continue;
                  }
                };

                #[cfg(feature = "metrics")]
                {
                  metrics::counter!("showbiz.packet.bytes.processing", self.metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
                }

                if let Err(e) = packet_tx.send(Packet::new(msg, addr, start)).await {
                  tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = %e, "failed to send packet");
                }

                #[cfg(feature = "metrics")]
                metrics::counter!("showbiz.packet.received", self.metric_labels.iter()).increment(n as u64);
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                tracing::error!(target: "showbiz.net.transport", peer=%local_addr, err = %e, "error reading UDP packet");
                continue;
              }
            };
          }
        }
      }
    });
  }

  async fn handle_remote_bytes(
    mut buf: BytesMut,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "encryption")] encryptor: Option<&Encryptor>,
    #[cfg(any(feature = "encryption", feature = "compression"))] offload_size: usize,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    let mut packet_label = buf.remove_label_header()?.unwrap_or_else(Label::empty);

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    let mut buf: Bytes = if ENCRYPT_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "encryption"))]
      return Err(NetTransportError::EncryptionDisabled);

      let encryptor = match encryptor {
        Some(encryptor) => encryptor,
        None => {
          return Err(SecurityError::Disabled.into());
        }
      };

      let buf_len = buf.len();
      if buf_len > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        {
          let keys = encryptor.kr.keys().await;
          let keyring = encryptor.kr.clone();
          let label = label.cheap_clone();
          rayon::spawn(move || {
            match keyring
              .decrypt_payload(&mut buf, keys, packet_label.as_bytes())
              .map_err(NetTransportError::Security)
            {
              Ok(_) => {}
              Err(e) => {
                if tx.send(Err(e)).is_err() {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send decryption result back to main thread");
                }
                return;
              }
            };

            if CHECKSUM_TAG.contains(&buf[0]) {
              let checksumer = match Checksumer::try_from(buf[0]) {
                Ok(checksumer) => checksumer,
                Err(e) => {
                  if tx.send(Err(e.into())).is_err() {
                    tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                  }
                  return;
                }
              };
              buf.advance(1);
              let expected = NetworkEndian::read_u32(&buf[..checksum::CHECKSUM_SIZE]);
              buf.advance(checksum::CHECKSUM_SIZE);
              let actual = checksumer.checksum(&buf);
              if actual != expected {
                if tx
                  .send(Err(NetTransportError::IO(Error::new(
                    ErrorKind::InvalidData,
                    "checksum mismatch",
                  ))))
                  .is_err()
                {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                }
                return;
              }
            }

            if COMPRESS_TAG.contains(&buf[0]) {
              #[cfg(not(feature = "compression"))]
              {
                if tx
                  .send(Err(NetTransportError::CompressionDisabled))
                  .is_err()
                {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                }
                return;
              }

              let compress_tag = buf[0];
              buf.advance(1);
              let compressor = match Compressor::try_from(compress_tag)
                .map_err(NetTransportError::UnkstartnCompressor)
              {
                Ok(compressor) => compressor,
                Err(e) => {
                  if tx.send(Err(e)).is_err() {
                    tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                  }
                  return;
                }
              };

              if tx
                .send(
                  compressor
                    .decompress(&buf)
                    .map(Bytes::from)
                    .map_err(NetTransportError::Decompress),
                )
                .is_err()
              {
                tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
              }
              return;
            }

            if tx.send(Ok(buf.freeze())).is_err() {
              tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send decryption result back to main thread");
            }
          });
        }

        match rx.await {
          Ok(Ok(buf)) => buf,
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(NetTransportError::ComputationTaskFailed),
        }
      } else {
        {
          let keys = encryptor.kr.keys().await;
          encryptor
            .kr
            .decrypt_payload(&mut buf, keys, packet_label.as_bytes())
            .map_err(NetTransportError::Security)?;
        }

        Self::check_checksum_and_decompress(buf, offload_size).await?
      }
    } else {
      Self::check_checksum_and_decompress(buf, offload_size).await?
    };

    if buf[0] == Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG {
      buf.advance(1);
      let num_msgs = buf[0] as usize;
      buf.advance(1);
      let mut msgs = OneOrMore::with_capacity(num_msgs);
      while msgs.len() != num_msgs {
        let msg_len = NetworkEndian::read_u16(&buf[..2]) as usize;
        buf.advance(2);
        let msg_bytes = buf.split_to(msg_len);
        let msg = <T::Wire as Wire>::decode_message(&msg_bytes).map_err(NetTransportError::Wire)?;
        msgs.push(msg);
      }
      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(&buf)
        .map(Into::into)
        .map_err(NetTransportError::Wire)
    }
  }

  async fn check_checksum_and_decompress(
    mut buf: BytesMut,
    offload_size: usize,
  ) -> Result<Bytes, NetTransportError<T::Resolver, T::Wire>> {
    if CHECKSUM_TAG.contains(&buf[0]) {
      let checksumer = Checksumer::try_from(buf[0])?;
      buf.advance(1);
      let expected = NetworkEndian::read_u32(&buf[..checksum::CHECKSUM_SIZE]);
      buf.advance(checksum::CHECKSUM_SIZE);
      let actual = checksumer.checksum(&buf);
      if actual != expected {
        return Err(NetTransportError::IO(Error::new(
          ErrorKind::InvalidData,
          "checksum mismatch",
        )));
      }
    }

    Ok(if COMPRESS_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "compression"))]
      return Err(NetTransportError::CompressionDisabled);

      let compress_tag = buf[0];
      buf.advance(1);
      let compressor = Compressor::try_from(compress_tag)?;

      if buf.len() > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        rayon::spawn(move || {
          let buf = compressor
            .decompress(&buf)
            .map_err(NetTransportError::Decompress);
          if tx.send(buf).is_err() {
            tracing::error!(target: "showbiz.net.packet", "failed to send back to main thread");
          }
        });
        match rx.await {
          Ok(Ok(buf)) => buf.into(),
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(NetTransportError::ComputationTaskFailed),
        }
      } else {
        compressor.decompress(&buf)?.into()
      }
    } else {
      buf.freeze()
    })
  }
}
