use super::*;

macro_rules! node_delegate_meta {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _node_delegate_meta >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_node_delegate_meta::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan(), t2_opts, Options::lan()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _node_delegate_meta_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_2".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_node_delegate_meta::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan(), t2_opts, Options::lan()).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _node_delegate_meta_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_1".into(), $expr).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_2".into(), $expr).with_primary_key(Some(TEST_KEYS[1]));
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_node_delegate_meta::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan(), t2_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(node_delegate_meta);
