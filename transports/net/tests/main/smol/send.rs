use crate::send_test_suites;

use super::*;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
send_test_suites!("tcp": Tcp<SmolRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
send_test_suites!("tls": Tls<SmolRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
}));

#[cfg(feature = "native-tls")]
send_test_suites!("native_tls": NativeTls<SmolRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
}));
