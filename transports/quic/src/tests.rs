#![allow(missing_docs, warnings)]

use core::panic;
use std::{future::Future, net::SocketAddr, sync::Arc};

use agnostic_lite::RuntimeLite;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{lock::Mutex, FutureExt, Stream};
use memberlist_core::{
  tests::AnyError,
  transport::{
    tests::{
      AddressKind, TestPacketClient, TestPacketConnection, TestPacketStream, TestPromisedClient,
      TestPromisedConnection, TestPromisedStream,
    },
    Transport,
  },
  types::{Label, LabelBufMutExt, Message},
};
use nodecraft::{CheapClone, Transformable};
use smol_str::SmolStr;

use crate::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

#[cfg(feature = "compression")]
use crate::compressor::Compressor;

/// Unit test for handling [`Ping`] message
pub mod handle_ping;

/// Unit test for handling compound ping message
#[cfg(feature = "compression")]
pub mod handle_compound_ping;

/// Unit test for handling indirect ping message
#[cfg(feature = "compression")]
pub mod handle_indirect_ping;

/// Unit test for handling ping from wrong node
#[cfg(feature = "compression")]
pub mod handle_ping_wrong_node;

/// Unit test for handling send packet with piggyback
#[cfg(feature = "compression")]
pub mod packet_piggyback;

/// Unit test for handling promised ping
#[cfg(feature = "compression")]
pub mod promised_ping;

/// Unit test for handling promised push pull
#[cfg(feature = "compression")]
pub mod promised_push_pull;

/// Unit test for sending
#[cfg(feature = "compression")]
pub mod send;

/// Unit test for joining
#[cfg(feature = "compression")]
pub mod join;

/// Unit test for joining dead node
pub mod join_dead_node;

pub struct QuicTestPacketStream<S: StreamLayer> {
  stream: S::Stream,
  addr: SocketAddr,
  label: Label,
  send_label: bool,
  #[cfg(feature = "compression")]
  send_compressed: Option<Compressor>,
  receive_verify_label: bool,
  #[cfg(feature = "compression")]
  receive_compressed: bool,
}

impl<S: StreamLayer> TestPacketStream for QuicTestPacketStream<S> {
  async fn send_to(&mut self, src: &[u8]) -> Result<(), AnyError> {
    let mut out = BytesMut::new();
    out.put_u8(1);
    if self.send_label {
      out.add_label_header(&self.label);
    }

    let mut data = BytesMut::new();

    #[cfg(feature = "compression")]
    if let Some(compressor) = self.send_compressed {
      data.put_u8(compressor as u8);
      let compressed = compressor.compress_into_bytes(src)?;
      let cur = data.len();
      // put compressed data length placeholder
      data.put_u32(0);
      NetworkEndian::write_u32(&mut data[cur..], compressed.len() as u32);
      data.put_slice(&compressed);
    } else {
      data.put_slice(src);
    }

    #[cfg(not(feature = "compression"))]
    data.put_slice(src);

    out.put_slice(&data);
    let stream = &mut self.stream;
    stream.write_all(out.freeze()).await?;
    let _ = stream.finish().await;
    Ok(())
  }

  async fn recv_from(&mut self) -> Result<(Bytes, SocketAddr), AnyError> {
    let stream = &mut self.stream;

    let mut buf = [0u8; 3];
    stream.peek_exact(&mut buf).await?;
    assert_eq!(buf[0], super::StreamType::Packet as u8);
    let mut drop = [0; 1];
    stream.read_exact(&mut drop).await?;

    if buf[1] == Label::TAG {
      let len = buf[2] as usize;
      let mut label_buf = vec![0u8; len];
      // consume the peeked data
      let mut drop = [0; 2];
      stream.read_exact(&mut drop).await.unwrap();
      stream.read_exact(&mut label_buf).await?;

      let label = Label::try_from(label_buf)?;
      if self.receive_verify_label {
        assert_eq!(label, self.label);
      }
    }

    #[cfg(feature = "compression")]
    if self.receive_compressed {
      let mut header = [0u8; 5];
      stream.read_exact(&mut header).await?;
      let compressor = Compressor::try_from(header[0])?;
      let compressed_data_len = NetworkEndian::read_u32(&header[1..]) as usize;
      let mut all = vec![0u8; compressed_data_len];
      stream.read_exact(&mut all).await?;
      let uncompressed = compressor.decompress(&all[..compressed_data_len])?;
      Ok((uncompressed.into(), self.addr))
    } else {
      let mut all = vec![0u8; 1500];
      let len = stream.read(&mut all).await?;
      all.truncate(len);
      Ok((all.into(), self.addr))
    }

    #[cfg(not(feature = "compression"))]
    {
      let mut all = vec![0u8; 1500];
      let len = stream.read(&mut all).await?;
      all.truncate(len);
      Ok((all.into(), self.addr))
    }
  }

  async fn finish(&mut self) -> Result<(), AnyError> {
    self.stream.finish().await?;
    Ok(())
  }
}

pub struct QuicTestPacketConnection<S: StreamLayer> {
  conn: S::Connection,
  addr: SocketAddr,
  label: Label,
  send_label: bool,
  #[cfg(feature = "compression")]
  send_compressed: Option<Compressor>,
  receive_verify_label: bool,
  #[cfg(feature = "compression")]
  receive_compressed: bool,
}

impl<S: StreamLayer> TestPacketConnection for QuicTestPacketConnection<S> {
  type Stream = QuicTestPacketStream<S>;

  async fn accept(&self) -> Result<Self::Stream, AnyError> {
    self
      .conn
      .accept_bi()
      .await
      .map(|(stream, _)| QuicTestPacketStream {
        stream,
        addr: self.addr,
        label: self.label.cheap_clone(),
        send_label: self.send_label,
        #[cfg(feature = "compression")]
        send_compressed: self.send_compressed,
        receive_verify_label: self.receive_verify_label,
        #[cfg(feature = "compression")]
        receive_compressed: self.receive_compressed,
      })
      .map_err(Into::into)
  }

  async fn connect(&self) -> Result<Self::Stream, AnyError> {
    self
      .conn
      .open_bi()
      .await
      .map(|(stream, _)| QuicTestPacketStream {
        stream,
        addr: self.addr,
        label: self.label.cheap_clone(),
        send_label: self.send_label,
        #[cfg(feature = "compression")]
        send_compressed: self.send_compressed,
        receive_verify_label: self.receive_verify_label,
        #[cfg(feature = "compression")]
        receive_compressed: self.receive_compressed,
      })
      .map_err(Into::into)
  }
}

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct QuicTransportTestClient<S: StreamLayer<Runtime = R>, R: RuntimeLite> {
  #[viewit(getter(skip), setter(skip))]
  connector: S::Connector,
  #[viewit(getter(skip), setter(skip))]
  acceptor: S::Acceptor,
  #[viewit(getter(skip), setter(skip))]
  local_addr: SocketAddr,
  #[viewit(getter(skip), setter(skip))]
  remote_addr: SocketAddr,

  label: Label,
  send_label: bool,

  #[cfg(feature = "compression")]
  #[viewit(
    getter(attrs(cfg(feature = "compression"))),
    setter(attrs(cfg(feature = "compression")))
  )]
  send_compressed: Option<Compressor>,
  receive_verify_label: bool,
  #[cfg(feature = "compression")]
  #[viewit(
    getter(attrs(cfg(feature = "compression"))),
    setter(attrs(cfg(feature = "compression")))
  )]
  receive_compressed: bool,

  #[viewit(getter(skip), setter(skip))]
  _runtime: std::marker::PhantomData<R>,
}

impl<S: StreamLayer<Runtime = R>, R: RuntimeLite> QuicTransportTestClient<S, R> {
  /// Creates a new test client with the given address
  pub async fn new(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    layer: S,
  ) -> Result<Self, AnyError> {
    Self::with_num_responses(local_addr, remote_addr, layer, 1).await
  }

  /// Creates a new test client with the given address
  pub async fn with_num_responses(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    layer: S,
    num_resps: usize,
  ) -> Result<Self, AnyError> {
    let (local_addr, mut acceptor, client) = layer.bind(local_addr).await?;

    Ok(Self {
      local_addr,
      remote_addr,
      connector: client,
      acceptor,
      label: Label::empty(),
      send_label: false,
      #[cfg(feature = "compression")]
      send_compressed: None,
      receive_verify_label: false,
      #[cfg(feature = "compression")]
      receive_compressed: false,
      _runtime: std::marker::PhantomData,
    })
  }
}

impl<S: StreamLayer<Runtime = R>, R: RuntimeLite> TestPacketClient
  for QuicTransportTestClient<S, R>
{
  type Connection = QuicTestPacketConnection<S>;

  async fn accept(&mut self) -> Result<Self::Connection, AnyError> {
    self
      .acceptor
      .accept()
      .await
      .map(|(conn, _)| QuicTestPacketConnection {
        conn,
        addr: self.local_addr,
        label: self.label.cheap_clone(),
        send_label: self.send_label,
        #[cfg(feature = "compression")]
        send_compressed: self.send_compressed,
        receive_verify_label: self.receive_verify_label,
        #[cfg(feature = "compression")]
        receive_compressed: self.receive_compressed,
      })
      .map_err(Into::into)
  }

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, AnyError> {
    self
      .connector
      .connect(addr)
      .await
      .map(|conn| QuicTestPacketConnection {
        conn,
        addr,
        label: self.label.cheap_clone(),
        send_label: self.send_label,
        #[cfg(feature = "compression")]
        send_compressed: self.send_compressed,
        receive_verify_label: self.receive_verify_label,
        #[cfg(feature = "compression")]
        receive_compressed: self.receive_compressed,
      })
      .map_err(Into::into)
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  async fn close(&mut self) {
    let _ = self.acceptor.close().await;
    let _ = self.connector.wait_idle().await;
    let _ = self.connector.close().await;
  }
}

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct QuicTransportTestPromisedClient<S: StreamLayer> {
  ln: Arc<Mutex<S::Acceptor>>,
  local_addr: SocketAddr,
  connector: S::Connector,
  connections: Arc<Mutex<Vec<S::Connection>>>,
  layer: S,
}

impl<S: StreamLayer> QuicTransportTestPromisedClient<S> {
  /// Creates a new test client with the given address
  pub fn new(layer: S, ln: S::Acceptor, connector: S::Connector) -> Self {
    let local_addr = ln.local_addr();
    Self {
      layer,
      ln: Arc::new(Mutex::new(ln)),
      connector,
      connections: Arc::new(Mutex::new(Vec::new())),
      local_addr,
    }
  }
}

pub struct QuicTestPromisedStream<S: StreamLayer> {
  stream: S::Stream,
}

impl<S: StreamLayer> TestPromisedStream for QuicTestPromisedStream<S> {
  async fn finish(&mut self) -> Result<(), AnyError> {
    self.stream.finish().await.map_err(Into::into)
  }
}

impl<S: StreamLayer> AsMut<S::Stream> for QuicTestPromisedStream<S> {
  fn as_mut(&mut self) -> &mut S::Stream {
    &mut self.stream
  }
}

pub struct QuicTestConnection<S: StreamLayer> {
  conn: S::Connection,
  addr: SocketAddr,
}

impl<S: StreamLayer> TestPromisedConnection for QuicTestConnection<S> {
  type Stream = QuicTestPromisedStream<S>;

  async fn accept(&self) -> Result<(Self::Stream, SocketAddr), AnyError> {
    self
      .conn
      .accept_bi()
      .await
      .map(|(s, _)| (QuicTestPromisedStream { stream: s }, self.addr))
      .map_err(Into::into)
  }

  async fn connect(&self) -> Result<Self::Stream, AnyError> {
    self
      .conn
      .open_bi()
      .await
      .map(|(s, _)| QuicTestPromisedStream { stream: s })
      .map_err(Into::into)
  }
}

impl<S: StreamLayer> TestPromisedClient for QuicTransportTestPromisedClient<S> {
  type Stream = QuicTestPromisedStream<S>;
  type Connection = QuicTestConnection<S>;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, AnyError> {
    self
      .connector
      .connect(addr)
      .await
      .map(|conn| QuicTestConnection { conn, addr })
      .map_err(Into::into)
  }

  async fn accept(&self) -> Result<Self::Connection, AnyError> {
    self
      .ln
      .lock()
      .await
      .accept()
      .await
      .map(|(conn, addr)| QuicTestConnection { conn, addr })
      .map_err(Into::into)
  }

  async fn close(&self) -> Result<(), AnyError> {
    self.ln.lock().await.close().await.map_err(Into::into)
  }

  fn local_addr(&self) -> std::io::Result<SocketAddr> {
    Ok(self.local_addr)
  }
}

/// A helper function to decompress data from the given source.
#[cfg(feature = "compression")]
pub fn read_compressed_data(src: &[u8]) -> Result<Vec<u8>, AnyError> {
  let compressor = Compressor::try_from(src[0])?;
  let compressed_data_len = NetworkEndian::read_u32(&src[1..]) as usize;
  assert_eq!(
    compressed_data_len,
    src.len() - 5,
    "compressed data length mismatch"
  );
  compressor.decompress(&src[5..]).map_err(Into::into)
}

fn compound_encoder(msgs: &[Message<SmolStr, SocketAddr>]) -> Result<Bytes, AnyError> {
  let num_msgs = msgs.len() as u8;
  let total_bytes = 6 + msgs.iter().map(|m| m.encoded_len() + 4).sum::<usize>();
  let mut out = BytesMut::with_capacity(total_bytes);
  out.put_u8(Message::<SmolStr, SocketAddr>::COMPOUND_TAG);
  out.put_u32(0);
  NetworkEndian::write_u32(&mut out[1..], total_bytes as u32);
  out.put_u8(num_msgs);

  let mut cur = out.len();
  out.resize(total_bytes, 0);

  for msg in msgs {
    let len = msg.encoded_len() as u32;
    NetworkEndian::write_u32(&mut out[cur..], len);
    cur += 4;
    let len = msg.encode(&mut out[cur..])?;
    cur += len;
  }

  Ok(out.freeze())
}

#[cfg(feature = "quinn")]
pub use quinn_stream_layer::*;

#[cfg(feature = "quinn")]
mod quinn_stream_layer {
  use super::*;
  use crate::stream_layer::quinn::*;
  use ::quinn::{ClientConfig, Endpoint, ServerConfig};
  use futures::Future;
  use quinn::{crypto::rustls::QuicClientConfig, EndpointConfig};
  use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
  use smol_str::SmolStr;
  use std::{
    error::Error,
    net::SocketAddr,
    sync::{
      atomic::{AtomicU16, Ordering},
      Arc,
    },
    time::Duration,
  };

  /// Dummy certificate verifier that treats any certificate as valid.
  /// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
  #[derive(Debug)]
  struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

  impl SkipServerVerification {
    fn new() -> Arc<Self> {
      Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
  }

  impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
      &self,
      _end_entity: &CertificateDer<'_>,
      _intermediates: &[CertificateDer<'_>],
      _server_name: &ServerName<'_>,
      _ocsp: &[u8],
      _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls12_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn verify_tls13_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls13_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn configures() -> Result<(ServerConfig, ClientConfig), Box<dyn Error + Send + Sync + 'static>> {
    let (server_config, _) = configure_server()?;
    let client_config = configure_client();
    Ok((server_config, client_config))
  }

  fn configure_client() -> ClientConfig {
    ClientConfig::new(Arc::new(
      QuicClientConfig::try_from(
        rustls::ClientConfig::builder()
          .dangerous()
          .with_custom_certificate_verifier(SkipServerVerification::new())
          .with_no_client_auth(),
      )
      .unwrap(),
    ))
  }

  // fn configure_server() -> Result<rustls::ServerConfig, Box<dyn Error>> {
  //   let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  //   let cert_der = cert.serialize_der().unwrap();
  //   let priv_key = cert.serialize_private_key_der();
  //   let priv_key = rustls::pki_types::pem::SectionKind::PrivateKey(priv_key);
  //   let cert_chain = vec![rustls::pki_types::pem::SectionKind::Certificat(
  //     cert_der.clone(),
  //   )];

  //   let mut cfg = rustls::ServerConfig::builder()
  //     .with_safe_default_cipher_suites()
  //     .with_safe_default_kx_groups()
  //     .with_protocol_versions(&[&rustls::version::TLS13])
  //     .unwrap()
  //     .with_no_client_auth()
  //     .with_single_cert(cert_chain, priv_key)?;
  //   cfg.max_early_data_size = u32::MAX;
  //   Ok(cfg)
  // }

  // fn configure_client(
  //   server_certs: &[&[u8]],
  // ) -> Result<ClientConfig, Box<dyn Error + Send + Sync + 'static>> {
  //   let mut certs = rustls::RootCertStore::empty();
  //   for cert in server_certs {
  //     certs.add(CertificateDer::from(*cert))?;
  //   }

  //   Ok(ClientConfig::with_root_certificates(Arc::new(certs))?)
  // }

  fn configure_server(
  ) -> Result<(ServerConfig, CertificateDer<'static>), Box<dyn Error + Send + Sync + 'static>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut server_config =
      ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
  }

  #[allow(unused)]
  const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer<R: RuntimeLite>() -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
  }

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer_with_connect_timeout<R: RuntimeLite>(
    timeout: Duration,
  ) -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
    .with_connect_timeout(timeout)
  }
}

#[cfg(feature = "s2n")]
pub use s2n_stream_layer::s2n_stream_layer;

#[cfg(feature = "s2n")]
mod s2n_stream_layer {
  use agnostic_lite::RuntimeLite;

  use crate::stream_layer::s2n::*;

  pub async fn s2n_stream_layer<R: RuntimeLite>() -> crate::s2n::Options {
    let p = std::env::current_dir().unwrap().join("tests");
    Options::new("localhost".into(), p.join("cert.pem"), p.join("key.pem"))
  }
}
