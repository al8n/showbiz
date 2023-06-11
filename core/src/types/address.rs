use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use super::*;

const V4_ADDR_SIZE: usize = 4;
const V6_ADDR_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct Domain(Bytes);

impl core::fmt::Display for Domain {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl Domain {
  /// The maximum length of a domain name, in bytes.
  pub const MAX_SIZE: usize = 253;
}

impl Default for Domain {
  fn default() -> Self {
    Self::new()
  }
}

impl Domain {
  #[inline]
  pub fn new() -> Self {
    Self(Bytes::new())
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
  pub(crate) fn from_bytes(s: &[u8]) -> Result<Self, InvalidDomain> {
    match core::str::from_utf8(s) {
      Ok(s) => Self::try_from(s),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn from_array<const N: usize>(s: [u8; N]) -> Result<Self, InvalidDomain> {
    match core::str::from_utf8(&s) {
      Ok(domain) => Self::try_from(domain),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    1 + self.0.len()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u8(self.0.len() as u8);
    buf.put(self.0.as_ref());
  }

  #[inline]
  pub(crate) fn decode_from(buf: Bytes) -> Result<Self, DecodeError> {
    if buf.is_empty() {
      return Err(DecodeError::Truncated("domain"));
    }

    let len = buf.remaining();
    if len > Self::MAX_SIZE {
      return Err(DecodeError::InvalidDomain(InvalidDomain::InvalidLength(
        len,
      )));
    }

    Self::try_from(buf).map_err(From::from)
  }
}

impl Serialize for Domain {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(self.as_str())
  }
}

impl<'de> Deserialize<'de> for Domain {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    Bytes::deserialize(deserializer)
      .and_then(|n| Domain::try_from(n).map_err(|e| serde::de::Error::custom(e)))
  }
}

impl AsRef<str> for Domain {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Domain {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
  }
}

impl core::cmp::Ord for Domain {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Domain {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Domain {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Domain {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Domain {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Domain {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Domain {}

impl core::hash::Hash for Domain {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl Domain {
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(&self.0).unwrap()
  }
}

impl TryFrom<&str> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    is_valid_domain_name(s).map(|_| Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    is_valid_domain_name(s.as_str()).map(|_| Self(s.into()))
  }
}

impl TryFrom<Bytes> for Domain {
  type Error = InvalidDomain;

  fn try_from(buf: Bytes) -> Result<Self, Self::Error> {
    match core::str::from_utf8(&buf) {
      Ok(s) => is_valid_domain_name(s).map(|_| Self(buf)),
      Err(e) => Err(InvalidDomain::Utf8(e)),
    }
  }
}

impl TryFrom<&Bytes> for Domain {
  type Error = InvalidDomain;

  fn try_from(buf: &Bytes) -> Result<Self, Self::Error> {
    match core::str::from_utf8(&buf) {
      Ok(s) => is_valid_domain_name(s).map(|_| Self(buf.clone())),
      Err(e) => Err(InvalidDomain::Utf8(e)),
    }
  }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidDomain {
  #[error("each label must be between 1 and 63 characters long, got {0}")]
  InvalidLabelSize(usize),
  #[error("{0}")]
  Other(&'static str),
  #[error("invalid domain length {0}, must be between 1 and 253")]
  InvalidLength(usize),
  #[error("invalid domain {0}")]
  Utf8(#[from] core::str::Utf8Error),
}

#[inline]
fn is_valid_domain_name(domain: &str) -> Result<(), InvalidDomain> {
  if domain.is_empty() || domain.len() > Domain::MAX_SIZE {
    return Err(InvalidDomain::InvalidLength(domain.len()));
  }

  for label in domain.split('.') {
    let len = label.len();
    // Each label must be between 1 and 63 characters long
    if len < 1 || len > 63 {
      return Err(InvalidDomain::InvalidLabelSize(len));
    }
    // Labels must start and end with an alphanumeric character
    if !label.chars().next().unwrap().is_alphanumeric()
      || !label.chars().last().unwrap().is_alphanumeric()
    {
      return Err(InvalidDomain::Other(
        "label must start and end with an alphanumeric character",
      ));
    }
    // Labels can contain alphanumeric characters and hyphens, but hyphen cannot be at the start or end
    if label.chars().any(|c| !c.is_alphanumeric() && c != '-') {
      return Err(InvalidDomain::Other("label can contain alphanumeric characters and hyphens, but hyphen cannot be at the start or end"));
    }
  }
  Ok(())
}

/// The Address for a node, can be an ip or a domain.
#[derive(Debug, Clone, Eq, Hash)]
pub enum NodeAddress {
  /// e.g. `128.0.0.1`
  Ip(IpAddr),
  /// e.g. `www.example.com`
  Domain(Domain),
}

impl PartialEq for NodeAddress {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Ip(a), Self::Ip(b)) => a == b,
      (Self::Domain(a), Self::Domain(b)) => a == b,
      (Self::Ip(a), Self::Domain(b)) => {
        if let Ok(addr) = b.as_str().parse::<IpAddr>() {
          a == &addr
        } else {
          false
        }
      }
      (Self::Domain(a), Self::Ip(b)) => {
        if let Ok(addr) = a.as_str().parse::<IpAddr>() {
          &addr == b
        } else {
          false
        }
      }
    }
  }
}

impl Default for NodeAddress {
  #[inline]
  fn default() -> Self {
    Self::Domain(Domain::new())
  }
}

impl NodeAddress {
  #[inline]
  pub const fn is_ip(&self) -> bool {
    matches!(self, Self::Ip(_))
  }

  #[inline]
  pub const fn is_domain(&self) -> bool {
    matches!(self, Self::Domain(_))
  }

  #[inline]
  pub fn unwrap_domain(&self) -> &str {
    match self {
      Self::Ip(_) => unreachable!(),
      Self::Domain(addr) => addr.as_str(),
    }
  }

  #[inline]
  pub(crate) const fn unwrap_ip(&self) -> IpAddr {
    match self {
      NodeAddress::Ip(addr) => *addr,
      _ => unreachable!(),
    }
  }

  #[inline]
  pub(crate) fn parse_ip(&self) -> Option<IpAddr> {
    match self {
      Self::Ip(addr) => Some(*addr),
      Self::Domain(addr) => addr.as_str().parse::<IpAddr>().ok(),
    }
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    1 // type mark
    + match self {
      Self::Ip(addr) => match addr {
        IpAddr::V4(_) => 4,
        IpAddr::V6(_) => 16,
      },
      Self::Domain(addr) => addr.encoded_len(),
    }
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u8(self.encoded_len() as u8);
    match self {
      Self::Ip(addr) => match addr {
        IpAddr::V4(addr) => {
          buf.put_u8(0);
          buf.put_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
          buf.put_u8(1);
          buf.put_slice(&addr.octets())
        }
      },
      Self::Domain(addr) => {
        buf.put_u8(2);
        addr.encode_to(buf);
      }
    }
  }

  #[inline]
  pub(crate) fn decode_len(mut buf: impl Buf) -> Result<usize, DecodeError> {
    if buf.remaining() < 1 {
      return Err(DecodeError::Corrupted);
    }

    Ok(buf.get_u8() as usize)
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    if buf.is_empty() {
      return Err(DecodeError::Truncated("node address"));
    }
    let mark = buf.get_u8();
    let remaining = buf.remaining();
    match mark {
      0 => match remaining {
        4 => {
          let mut addr = [0; V4_ADDR_SIZE];
          buf.copy_to_slice(&mut addr);
          Ok(Self::Ip(IpAddr::V4(Ipv4Addr::from(addr))))
        }
        r => Err(DecodeError::InvalidIpAddrLength(r)),
      },
      1 => match remaining {
        16 => {
          let mut addr = [0; V6_ADDR_SIZE];
          buf.copy_to_slice(&mut addr);
          Ok(Self::Ip(IpAddr::V6(Ipv6Addr::from(addr))))
        }
        r => Err(DecodeError::InvalidIpAddrLength(r)),
      },
      2 => Domain::decode_from(buf).map(Self::Domain),
      b => Err(DecodeError::UnknownMarkBit(b)),
    }
  }
}

impl From<IpAddr> for NodeAddress {
  fn from(addr: IpAddr) -> Self {
    Self::Ip(addr)
  }
}

impl From<Domain> for NodeAddress {
  fn from(addr: Domain) -> Self {
    match addr.as_str().parse() {
      Ok(ip) => Self::Ip(ip),
      Err(_) => Self::Domain(addr),
    }
  }
}

impl core::fmt::Display for NodeAddress {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Ip(addr) => write!(f, "{}", addr),
      Self::Domain(addr) => write!(f, "{}", addr),
    }
  }
}
