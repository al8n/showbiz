use super::DecodeError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

mod random;

/// Error returned when a name is invalid.
#[derive(Debug, thiserror::Error)]
pub enum InvalidName {
  /// name size is too large.
  #[error("name too large, max length of name is 512 but got {0}")]
  TooLarge(usize),
  /// name is empty.
  #[error("name cannot be empty")]
  Empty,
  /// name is not valid utf8.
  #[error("{0}")]
  Utf8(#[from] core::str::Utf8Error),
}

/// The name of a node in the showbiz.
///
/// The name is a string with a minimum length 1 and maximum length of 512 bytes.
#[derive(Clone)]
pub struct Name(Bytes);

impl Default for Name {
  #[inline]
  fn default() -> Self {
    Self::from_string_unchecked(random::random_name())
  }
}

impl Name {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = 512;

  /// Creates a new Name from a static str.
  #[inline]
  pub const fn from_static(name: &'static str) -> Result<Self, InvalidName> {
    if name.is_empty() {
      return Err(InvalidName::Empty);
    }

    if name.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(name.len()));
    }

    Ok(Self(Bytes::from_static(name.as_bytes())))
  }

  /// Creates a new Name from a static str.
  ///
  /// # Panics
  /// Panics if the name is empty or larger than 512 bytes.
  #[inline]
  pub const fn from_static_unchecked(name: &'static str) -> Self {
    if name.is_empty() {
      panic!("name is empty");
    }

    if name.len() > Self::MAX_SIZE {
      panic!("name is too large, max length of name is 512");
    }

    Self(Bytes::from_static(name.as_bytes()))
  }

  /// Creates a new Name from a str.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn from_str(name: &str) -> Result<Self, InvalidName> {
    if name.is_empty() {
      return Err(InvalidName::Empty);
    }

    if name.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(name.len()));
    }

    Ok(Self(Bytes::copy_from_slice(name.as_bytes())))
  }

  /// Creates a new Name from a str without checking the size.
  ///
  /// # Panics
  /// Panics if the name is empty or larger than 512 bytes.
  #[inline]
  pub fn from_str_unchecked(name: &str) -> Self {
    if name.is_empty() {
      panic!("{}", InvalidName::Empty);
    }

    if name.len() > Self::MAX_SIZE {
      panic!("{}", InvalidName::TooLarge(name.len()));
    }

    Self(Bytes::copy_from_slice(name.as_bytes()))
  }

  /// Creates a new Name from a String.
  #[inline]
  pub fn from_string(name: String) -> Result<Self, InvalidName> {
    if name.is_empty() {
      return Err(InvalidName::Empty);
    }

    if name.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(name.len()));
    }

    Ok(Self(Bytes::from(name)))
  }

  /// Creates a new Name from a String.
  ///
  /// # Panics
  /// Panics if the name is empty or larger than 512 bytes.
  #[inline]
  pub fn from_string_unchecked(name: String) -> Self {
    if name.is_empty() {
      panic!("{}", InvalidName::Empty);
    }

    if name.len() > Self::MAX_SIZE {
      panic!("{}", InvalidName::TooLarge(name.len()));
    }

    Self(Bytes::from(name))
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    core::mem::size_of::<u16>() + self.0.len()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u16(self.0.len() as u16);
    buf.put(self.0.as_ref());
  }

  #[inline]
  pub(crate) fn decode_len(mut buf: impl Buf) -> Result<usize, DecodeError> {
    if buf.remaining() < core::mem::size_of::<u16>() {
      return Err(DecodeError::Corrupted);
    }
    Ok(buf.get_u16() as usize)
  }

  #[inline]
  pub(crate) fn decode_from(buf: Bytes) -> Result<Self, DecodeError> {
    Self::try_from(buf).map_err(From::from)
  }

  /// Returns a reference to the underlying str of the name.
  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(self.0.as_ref()).unwrap()
  }

  /// Returns a reference to the underlying bytes of the name.
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }
}

impl Serialize for Name {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
      serializer.serialize_str(self.as_str())
    } else {
      serializer.serialize_bytes(self.as_bytes())
    }
  }
}

impl<'de> Deserialize<'de> for Name {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    if deserializer.is_human_readable() {
      String::deserialize(deserializer)
        .and_then(|n| Name::from_string(n).map_err(serde::de::Error::custom))
    } else {
      Bytes::deserialize(deserializer)
        .and_then(|n| Name::try_from(n).map_err(serde::de::Error::custom))
    }
  }
}

impl AsRef<str> for Name {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Name {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
  }
}

impl core::cmp::Ord for Name {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Name {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Name {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Name {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Name {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Name {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Name {}

impl core::hash::Hash for Name {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl TryFrom<&str> for Name {
  type Error = InvalidName;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    Self::from_str(s)
  }
}

impl TryFrom<Bytes> for Name {
  type Error = InvalidName;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    let len = s.len();
    if len == 0 {
      return Err(InvalidName::Empty);
    }

    if len > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s))
  }
}

impl TryFrom<&Bytes> for Name {
  type Error = InvalidName;

  fn try_from(s: &Bytes) -> Result<Self, Self::Error> {
    let len = s.len();
    if len == 0 {
      return Err(InvalidName::Empty);
    }

    if len > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s.clone()))
  }
}

impl core::fmt::Debug for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl core::fmt::Display for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}
