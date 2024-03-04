use super::*;

macro_rules! probe_node_awareness_improved {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_improved >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_2".into());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          probe_node_awareness_improved(t1, t1_opts, t2).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_improved_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_1".into()).with_compressor(Some(Default::default()));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_2".into()).with_compressor(Some(Default::default()));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          probe_node_awareness_improved(t1, t1_opts, t2).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_improved_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_1".into()).with_primary_key(Some(TEST_KEYS[0]));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_2".into()).with_primary_key(Some(TEST_KEYS[1]));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          probe_node_awareness_improved(t1, t1_opts, t2).await;
        });
      }

      #[cfg(all(feature = "encryption", feature = "compression"))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_improved_with_encryption_and_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_1".into()).with_primary_key(Some(TEST_KEYS[0])).with_compressor(Some(Default::default()));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("probe_node_awareness_improved_2".into()).with_primary_key(Some(TEST_KEYS[1])).with_compressor(Some(Default::default()));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          probe_node_awareness_improved(t1, t1_opts, t2).await;
        });
      }
    }
  };
}

test_mods!(probe_node_awareness_improved);
