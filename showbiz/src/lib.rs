#![forbid(unsafe_code)]
#[cfg(not(feature = "async"))]
compile_error!("showbiz does not support sync currently, `async` feature must be enabled.");

pub use agnostic;
pub use showbiz_core::*;

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tokio", not(target_family = "wasm")))))]
pub type TokioShowbiz<D> = showbiz_core::Showbiz<
  D,
  showbiz_core::transport::net::NetTransport<agnostic::tokio::TokioRuntime>,
  agnostic::tokio::TokioRuntime,
>;

#[cfg(all(feature = "async-std", not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(feature = "async-std", not(target_family = "wasm"))))
)]
pub type AsyncStdShowbiz<D> = showbiz_core::Showbiz<
  D,
  showbiz_core::transport::net::NetTransport<agnostic::async_std::AsyncStdRuntime>,
  agnostic::async_std::AsyncStdRuntime,
>;

#[cfg(all(feature = "smol", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "smol", not(target_family = "wasm")))))]
pub type SmolShowbiz<D> = showbiz_core::Showbiz<
  D,
  showbiz_core::transport::net::NetTransport<agnostic::smol::SmolRuntime>,
  agnostic::smol::SmolRuntime,
>;
