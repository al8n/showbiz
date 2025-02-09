use memberlist_core::transport::{tests::promised_ping as promised_ping_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn promised_ping<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label)
    .with_offload_size(10);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_no_label<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_no_label");

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_offload_size(10);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_compression_only<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_compression_only");

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_label_and_compression<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_label_and_compression");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_no_label_no_compression<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_no_compression");

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_label_only<S, R>(
  s: S::Options,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_label_only");
  let label = Label::try_from(&name)?;

  let mut opts =
    QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s).with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}
