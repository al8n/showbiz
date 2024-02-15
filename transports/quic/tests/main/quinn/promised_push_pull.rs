use super::*;
use crate::promised_push_pull_test_suites;

#[cfg(feature = "tokio")]
promised_push_pull_test_suites!("quinn_tokio": TokioRuntime::tokio_run({
  quinn_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "async-std")]
promised_push_pull_test_suites!("quinn_async_std": AsyncStdRuntime::async_std_run({
  quinn_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "smol")]
promised_push_pull_test_suites!("quinn_smol": SmolRuntime::smol_run({
  quinn_stream_layer::<SmolRuntime>().await
}));
