use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Bytes, BytesMut};
use nodecraft::{CheapClone, Transformable};

/// Invalid meta error.
#[derive(Debug, thiserror::Error)]
#[error("the size of meta must between [0-255] bytes, got {0}")]
pub struct LargeMeta(usize);

/// The metadata of a node in the cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(
  feature = "rkyv",
  rkyv(derive(Debug, PartialEq, Eq, Hash), compare(PartialEq))
)]
pub struct Meta(Bytes);

impl Default for Meta {
  #[inline]
  fn default() -> Self {
    Self::empty()
  }
}

impl CheapClone for Meta {}

impl Meta {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = 512;

  /// Create an empty meta.
  #[inline]
  pub const fn empty() -> Meta {
    Meta(Bytes::new())
  }

  /// Create a meta from a static str.
  #[inline]
  pub const fn from_static_str(s: &'static str) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s.as_bytes())))
  }

  /// Create a meta from a static bytes.
  #[inline]
  pub const fn from_static(s: &'static [u8]) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s)))
  }

  /// Returns the meta as a byte slice.
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  /// Returns true if the meta is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the length of the meta in bytes.
  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }
}

impl AsRef<[u8]> for Meta {
  fn as_ref(&self) -> &[u8] {
    self.as_bytes()
  }
}

impl core::ops::Deref for Meta {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_bytes()
  }
}

impl core::cmp::PartialEq<[u8]> for Meta {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_bytes().eq(other)
  }
}

impl core::cmp::PartialEq<&[u8]> for Meta {
  fn eq(&self, other: &&[u8]) -> bool {
    self.as_bytes().eq(*other)
  }
}

impl core::cmp::PartialEq<Bytes> for Meta {
  fn eq(&self, other: &Bytes) -> bool {
    self.as_bytes().eq(other.as_ref())
  }
}

impl core::cmp::PartialEq<Vec<u8>> for Meta {
  fn eq(&self, other: &Vec<u8>) -> bool {
    self.as_bytes().eq(other.as_slice())
  }
}

impl TryFrom<&str> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Meta {
  type Error = LargeMeta;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    Meta::try_from(s.into_bytes())
  }
}

impl TryFrom<Bytes> for Meta {
  type Error = LargeMeta;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s))
  }
}

impl TryFrom<Vec<u8>> for Meta {
  type Error = LargeMeta;

  fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
    Meta::try_from(Bytes::from(s))
  }
}

impl TryFrom<&[u8]> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s)))
  }
}

impl TryFrom<&Bytes> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s.clone()))
  }
}

impl TryFrom<BytesMut> for Meta {
  type Error = LargeMeta;

  fn try_from(s: BytesMut) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s.freeze()))
  }
}

/// Meta error.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
  /// Invalid meta.
  #[error(transparent)]
  LargeMeta(#[from] LargeMeta),
  /// Not enough bytes to decode meta.
  #[error("not enough bytes to decode meta")]
  NotEnoughBytes,
  /// Encode buffer too small.
  #[error("the buffer did not contain enough bytes to encode meta")]
  BufferTooSmall,
}

impl Transformable for Meta {
  type Error = MetaError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < self.encoded_len() {
      return Err(Self::Error::BufferTooSmall);
    }

    NetworkEndian::write_u16(dst, encoded_len as u16);
    dst[2..encoded_len].copy_from_slice(self.as_bytes());
    Ok(encoded_len)
  }

  fn encoded_len(&self) -> usize {
    core::mem::size_of::<u16>() + self.len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < core::mem::size_of::<u16>() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = NetworkEndian::read_u16(&src[0..2]) as usize;
    if len > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    Self::try_from(&src[2..len])
      .map_err(Self::Error::LargeMeta)
      .map(|meta| (len, meta))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_try_from_string() {
    let meta = Meta::try_from("hello".to_string()).unwrap();
    assert_eq!(meta, Meta::from_static_str("hello").unwrap());
    assert!(Meta::from_static([0; 513].as_slice()).is_err());
  }

  #[test]
  fn test_try_from_bytes() {
    let meta = Meta::try_from(Bytes::from("hello")).unwrap();
    assert_eq!(meta, Bytes::from("hello"));

    assert!(Meta::try_from(Bytes::from("a".repeat(513).into_bytes())).is_err());
  }

  #[test]
  fn test_try_from_bytes_mut() {
    let meta = Meta::try_from(BytesMut::from("hello")).unwrap();
    assert_eq!(meta, "hello".as_bytes().to_vec());

    assert!(Meta::try_from(BytesMut::from([0; 513].as_slice())).is_err());
  }

  #[test]
  fn test_try_from_bytes_ref() {
    let meta = Meta::try_from(&Bytes::from("hello")).unwrap();
    assert_eq!(meta, "hello".as_bytes());

    assert!(Meta::try_from(&Bytes::from("a".repeat(513).into_bytes())).is_err());
  }

  #[test]
  fn test_default() {
    let meta = Meta::default();
    assert!(meta.is_empty());

    assert_eq!(Meta::empty().as_ref(), &[]);
  }
}
