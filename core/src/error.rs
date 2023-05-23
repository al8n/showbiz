use showbiz_traits::{Delegate, Transport};
use showbiz_types::InvalidMessageType;

use crate::util::InvalidAddress;

#[derive(Debug, thiserror::Error)]
pub enum Error<T: Transport, D: Delegate> {
  #[error("showbiz: empty node name provided")]
  EmptyNodeName,
  #[error("showbiz: label is too long. expected at most 255 bytes, got {0}")]
  LabelTooLong(usize),
  #[error("showbiz: invalid message type {0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  #[error("showbiz: invalid address {0}")]
  InvalidAddress(#[from] InvalidAddress),
  #[error("showbiz: cannot decode label; packet has been truncated")]
  TruncatedLabel,
  #[error("showbiz: label header cannot be empty when present")]
  EmptyLabel,
  #[error("showbiz: io error {0}")]
  IO(#[from] std::io::Error),
  #[error("showbiz: remote node state(size {0}) is larger than limit")]
  LargeRemoteState(usize),
  #[error("showbiz: security error {0}")]
  Security(#[from] crate::security::SecurityError),
  #[error("showbiz: node names are required by configuration but one was not provided")]
  MissingNodeName,
  #[error("showbiz: {0}")]
  Compression(#[from] crate::util::CompressError),
  #[error("showbiz: {0}")]
  Encode(#[from] prost::EncodeError),
  #[error("showbiz: {0}")]
  Decode(#[from] prost::DecodeError),
  #[error("showbiz: timeout waiting for update broadcast")]
  UpdateTimeout,
  #[error("showbiz: dns error: {0}")]
  DNS(#[from] trust_dns_resolver::error::ResolveError),
  #[error("showbiz: {0}")]
  Delegate(D::Error),
  #[error("showbiz: {0}")]
  Transport(T::Error),
}

impl<D: Delegate, T: Transport> Error<T, D> {
  #[inline]
  pub fn delegate(e: D::Error) -> Self {
    Self::Delegate(e)
  }

  #[inline]
  pub fn transport(e: T::Error) -> Self {
    Self::Transport(e)
  }

  #[inline]
  pub fn dns(e: trust_dns_resolver::error::ResolveError) -> Self {
    Self::DNS(e)
  }
}

impl<D: Delegate, T: Transport> PartialEq for Error<T, D> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::LabelTooLong(a), Self::LabelTooLong(b)) => a == b,
      (Self::InvalidMessageType(a), Self::InvalidMessageType(b)) => a == b,
      (Self::TruncatedLabel, Self::TruncatedLabel) => true,
      (Self::EmptyLabel, Self::EmptyLabel) => true,
      (Self::IO(a), Self::IO(b)) => a.kind() == b.kind(),
      _ => false,
    }
  }
}
