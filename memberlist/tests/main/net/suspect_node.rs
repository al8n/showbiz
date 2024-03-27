use super::*;

macro_rules! suspect_node {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options_options("suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));
          let opts = Options::lan();

          suspect_node(t1_opts, opts, "test".into()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options_options("suspect_node_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          suspect_node(t1_opts, opts, "test".into()).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1 = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options_options("suspect_node_1".into(), $expr).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10);
          t1.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          suspect_node(t1, opts, "test".into()).await;
        });
      }

      #[cfg(all(feature = "encryption", feature = "compression"))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node_with_encryption_and_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1 = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options_options("suspect_node_1".into(), $expr).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10).with_compressor(Some(Default::default()));
          t1.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          suspect_node(t1, opts, "test".into()).await;
        });
      }
    }
  };
}

test_mods!(suspect_node);
