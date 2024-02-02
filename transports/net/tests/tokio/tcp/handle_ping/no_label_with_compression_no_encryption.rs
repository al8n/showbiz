use super::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into()).with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v4());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption".into()).with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v6());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      verify_checksum(src)?;
      let uncompressed = read_compressed_data(&src[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
));
