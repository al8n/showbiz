use super::*;
use crate::handle_ping_no_label_no_compression_test_suites;

handle_ping_no_label_no_compression_test_suites!("s2n": S2n<TokioRuntime>::run({
  s2n_stream_layer::<TokioRuntime>().await
}));
