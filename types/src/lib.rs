pub use ack::*;
pub use alive::*;
pub use bad_state::*;
pub use cidr_policy::*;
pub use data::*;
pub use err::*;
pub use label::*;
pub use message::*;
pub use meta::*;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use metrics_label::MetricLabels;
pub use nodecraft::{
  Domain, HostAddr, Node, NodeId, ParseDomainError, ParseHostAddrError, ParseNodeIdError,
};
pub use ping::*;
pub use push_pull_state::*;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use secret::*;
pub use server::*;
pub use smallvec_wrapper::*;
pub use version::*;

mod ack;
mod alive;
mod bad_state;
mod cidr_policy;
mod data;
mod err;
mod label;
mod message;
mod meta;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
mod metrics_label;
mod ping;
mod push_pull_state;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
mod secret;
mod server;
mod version;

// const MAX_ENCODED_LEN_SIZE: usize = core::mem::size_of::<u32>();

/// A wire type used in Protobuf-like encoding/decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum WireType {
  /// A byte wire type.
  Byte = 0,
  /// A varint wire type.
  Varint = 1,
  /// A length-delimited wire type.
  LengthDelimited = 2,
  /// Fixed 32-bit wire type.
  Fixed32 = 3,
  /// Fixed 64-bit wire type.
  Fixed64 = 4,
}

impl WireType {
  /// Returns the [`WireType`] as a `&'static str`.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Byte => "byte",
      Self::Varint => "varint",
      Self::LengthDelimited => "length-delimited",
      Self::Fixed32 => "fixed32",
      Self::Fixed64 => "fixed64",
    }
  }
}

impl core::fmt::Display for WireType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl TryFrom<u8> for WireType {
  type Error = u8;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Byte,
      1 => Self::Varint,
      2 => Self::LengthDelimited,
      _ => return Err(value),
    })
  }
}

#[inline]
const fn merge(ty: WireType, tag: u8) -> u8 {
  (ty as u8) << 3 | tag
}

#[inline]
const fn split(val: u8) -> (u8, u8) {
  let wire_type = val >> 3; // Shift right to get the wire type
  let tag = val & 0b111; // Mask with 0b111 to get last 3 bits
  (wire_type, tag)
}

fn skip(wire_type: WireType, src: &[u8]) -> Result<usize, DecodeError> {
  match wire_type {
    WireType::Varint => match const_varint::decode_u64_varint(src) {
      Ok((bytes_read, _)) => Ok(bytes_read),
      Err(_) => Err(DecodeError::new("invalid varint")),
    },
    WireType::LengthDelimited => {
      // Skip length-delimited field by reading the length and skipping the payload
      if src.is_empty() {
        return Err(DecodeError::new("buffer underflow"));
      }

      match const_varint::decode_u32_varint(src) {
        Ok((bytes_read, length)) => Ok(bytes_read + length as usize),
        Err(_) => Err(DecodeError::new("invalid varint")),
      }
    }
    WireType::Byte => Ok(1),
    WireType::Fixed32 => Ok(4),
    WireType::Fixed64 => Ok(8),
  }
}

#[inline]
fn debug_assert_write_eq(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual, expected,
    "expect writting {expected} bytes, but actual write {actual} bytes"
  );
}

#[inline]
fn debug_assert_read_eq(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual, expected,
    "expect reading {expected} bytes, but actual read {actual} bytes"
  );
}

#[inline]
fn check_encoded_message_size(required: usize) -> Result<(), EncodeError> {
  if required > u32::MAX as usize {
    return Err(EncodeError::TooLarge);
  }

  Ok(())
}
