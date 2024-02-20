use super::*;

macro_rules! invoke_ack_handler_channel_nack {
  ($rt: ident) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _invoke_ack_handler_channel_nack >]() {
        [< $rt:snake _run >](async move {
          invoke_ack_handler_channel_nack::<[< $rt:camel Runtime >]>().await;
        });
      }
    }
  };
}

#[cfg(feature = "tokio")]
mod tokio {
  use agnostic::tokio::TokioRuntime;

  use super::*;
  use crate::tokio_run;

  invoke_ack_handler_channel_nack!(tokio);
}

#[cfg(feature = "async-std")]
mod async_std {
  use agnostic::async_std::AsyncStdRuntime;

  use super::*;
  use crate::async_std_run;

  invoke_ack_handler_channel_nack!(async_std);
}

#[cfg(feature = "smol")]
mod smol {
  use agnostic::smol::SmolRuntime;

  use super::*;
  use crate::smol_run;

  invoke_ack_handler_channel_nack!(smol);
}
