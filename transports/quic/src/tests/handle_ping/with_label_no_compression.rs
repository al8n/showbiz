use super::*;

pub async fn server_with_label_no_compression_client_with_label_no_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name =
    format!("{kind}_ping_server_with_label_no_compression_client_with_label_no_compression");
  let label = Label::try_from(&name)?;
  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;

  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(kind.next(0), *remote_addr, c)
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);

  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_with_label_no_compression_client_no_label_no_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_ping_server_with_label_no_compression_client_no_label_no_compression");
  let label = Label::try_from(&name)?;
  let label = Label::try_from(&name)?;
  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_label(label.cheap_clone())
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;

  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(kind.next(0), *remote_addr, c)
    .await?
    .with_label(label.cheap_clone())
    .with_receive_verify_label(true);

  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_no_label_no_compression_client_with_label_no_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  let name = format!("{kind}_ping_server_no_label_no_compression_client_with_label_no_compression");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_skip_inbound_label_check(server_check_label);
  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true);
  handle_ping(trans, tc).await?;
  Ok(())
}
