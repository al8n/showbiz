use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::handle_indirect_ping::indirect_ping;

unit_tests_with_expr!(run(
  v4_indirect_ping({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = indirect_ping::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  v6_indirect_ping({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = indirect_ping::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  })
));
