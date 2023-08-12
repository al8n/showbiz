use std::{
  net::{IpAddr, SocketAddr},
  ops::{Deref, DerefMut},
  time::Instant,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::version::VSN_SIZE;

const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

/// Returns the encoded length of the value in LEB128 variable length format.
/// The returned value will be between 1 and 5, inclusive.
#[inline]
pub(crate) fn encoded_u32_len(value: u32) -> usize {
  ((((value as u64 | 1).leading_zeros() ^ 63) * 9 + 73) / 64) as usize
}

#[inline]
pub(crate) fn encode_u32_to_buf(mut buf: impl BufMut, mut n: u32) {
  while n >= 0x80 {
    buf.put_u8(((n as u8) & 0x7F) | 0x80);
    n >>= 7;
  }
  buf.put_u8(n as u8);
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeU32Error;

impl core::fmt::Display for DecodeU32Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid length")
  }
}

impl std::error::Error for DecodeU32Error {}

#[inline]
pub(crate) fn decode_u32_from_buf(mut buf: impl Buf) -> Result<(u32, usize), DecodeU32Error> {
  let mut n = 0;
  let mut shift = 0;
  let mut i = 0;
  while buf.has_remaining() {
    let b = buf.get_u8();
    i += 1;
    if b < 0x80 {
      return Ok((n | ((b as u32) << shift), i));
    }

    n |= ((b & 0x7f) as u32) << shift;
    if shift >= 25 {
      return Err(DecodeU32Error);
    }
    shift += 7;
  }

  Err(DecodeU32Error)
}

// mod compress;
// pub use compress::*;

// mod name;
// pub use name::*;

mod address;
pub use address::*;

// mod id;
// pub use id::*;

// mod ack;
// pub(crate) use ack::*;

// mod alive;
// pub(crate) use alive::*;

// mod bad_state;
// pub(crate) use bad_state::*;

// mod err;
// pub(crate) use err::*;

// mod ping;
// pub(crate) use ping::*;

// mod push_pull_state;
// pub(crate) use push_pull_state::*;

mod label;
pub use label::*;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
  #[error("truncated {0}")]
  Truncated(&'static str),
  #[error("corrupted message")]
  Corrupted,
  #[error("checksum mismatch")]
  ChecksumMismatch,
  #[error("fail to decode length {0}")]
  Length(DecodeU32Error),
  #[error("unknown mark bit {0}")]
  UnknownMarkBit(u8),
  #[error("invalid ip addr length {0}")]
  InvalidIpAddrLength(usize),
  #[error("invalid domain {0}")]
  InvalidDomain(#[from] InvalidDomain),
  // #[error("invalid name {0}")]
  // InvalidName(#[from] InvalidName),
  #[error("invalid string {0}")]
  InvalidErrorResponse(std::string::FromUtf8Error),
  #[error("invalid size {0}")]
  InvalidMessageSize(#[from] DecodeU32Error),
  #[error("{0}")]
  InvalidNodeState(#[from] InvalidNodeState),
  #[error("{0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  // #[error("{0}")]
  // InvalidCompressionAlgo(#[from] InvalidCompressionAlgo),
  #[error("{0}")]
  InvalidLabel(#[from] InvalidLabel),
  #[error("failed to read full push node state ({0} / {1})")]
  FailReadRemoteState(usize, usize),
  #[error("failed to read full user state ({0} / {1})")]
  FailReadUserState(usize, usize),
  #[error("mismatch message type, expected {expected}, got {got}")]
  MismatchMessageType {
    expected: &'static str,
    got: &'static str,
  },
  #[error("sequence number from ack ({ack}) doesn't match ping ({ping})")]
  MismatchSequenceNumber { ack: u32, ping: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
  #[error("{0}")]
  InvalidLabel(#[from] InvalidLabel),
}

#[viewit::viewit]
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct UserMsgHeader {
  len: u32, // Encodes the byte lengh of user state
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Message(pub(crate) BytesMut);

impl Default for Message {
  fn default() -> Self {
    Self::new()
  }
}

impl Message {
  const PREFIX_SIZE: usize = 1;

  #[inline]
  pub fn new() -> Self {
    Self::new_with_type(MessageType::User)
  }

  #[doc(hidden)]
  #[inline]
  pub fn __from_bytes_mut(data: BytesMut) -> Self {
    Self(data)
  }

  #[inline]
  pub(crate) fn new_with_type(ty: MessageType) -> Self {
    let mut this = BytesMut::with_capacity(Self::PREFIX_SIZE);
    this.put_u8(ty as u8);
    Self(this)
  }

  pub(crate) fn compounds(mut msgs: Vec<Self>) -> Vec<BytesMut> {
    const MAX_MESSAGES: usize = 255;

    let mut bufs = Vec::with_capacity((msgs.len() + MAX_MESSAGES - 1) / MAX_MESSAGES);

    while msgs.len() > MAX_MESSAGES {
      bufs.push(Self::compound(msgs.drain(..MAX_MESSAGES).collect()));
    }

    if !msgs.is_empty() {
      bufs.push(Self::compound(msgs));
    }

    bufs
  }

  pub(crate) fn compound(msgs: Vec<Self>) -> BytesMut {
    let num_msgs = msgs.len();
    let total: usize = msgs.iter().map(|m| m.len()).sum();
    let mut buf = BytesMut::with_capacity(
      MessageType::SIZE
        + core::mem::size_of::<u8>()
        + num_msgs * core::mem::size_of::<u16>()
        + total,
    );
    // Write out the type
    buf.put_u8(MessageType::Compound as u8);
    // Write out the number of message
    buf.put_u8(num_msgs as u8);

    let mut compound = buf.split_off(num_msgs * 2);
    for msg in msgs {
      // Add the message length
      buf.put_u16(msg.len() as u16);
      // put msg into compound
      compound.put_slice(&msg.freeze());
    }

    buf.unsplit(compound);
    buf
  }

  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut this = BytesMut::with_capacity(cap + Self::PREFIX_SIZE);
    this.put_u8(MessageType::User as u8);
    Self(this)
  }

  #[inline]
  pub fn resize(&mut self, new_len: usize, val: u8) {
    self.0.resize(new_len + Self::PREFIX_SIZE, val);
  }

  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.0.reserve(additional);
  }

  #[inline]
  pub fn remaining(&self) -> usize {
    self.0.remaining()
  }

  #[inline]
  pub fn remaining_mut(&self) -> usize {
    self.0.remaining_mut()
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.0.truncate(len + Self::PREFIX_SIZE);
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.0.put_slice(buf);
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.0.put_u8(val);
  }

  #[inline]
  pub fn put_u16(&mut self, val: u16) {
    self.0.put_u16(val);
  }

  #[inline]
  pub fn put_u16_le(&mut self, val: u16) {
    self.0.put_u16_le(val);
  }

  #[inline]
  pub fn put_u32(&mut self, val: u32) {
    self.0.put_u32(val);
  }

  #[inline]
  pub fn put_u32_le(&mut self, val: u32) {
    self.0.put_u32_le(val);
  }

  #[inline]
  pub fn put_u64(&mut self, val: u64) {
    self.0.put_u64(val);
  }

  #[inline]
  pub fn put_u64_le(&mut self, val: u64) {
    self.0.put_u64_le(val);
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.0.put_i8(val);
  }

  #[inline]
  pub fn put_i16(&mut self, val: i16) {
    self.0.put_i16(val);
  }

  #[inline]
  pub fn put_i16_le(&mut self, val: i16) {
    self.0.put_i16_le(val);
  }

  #[inline]
  pub fn put_i32(&mut self, val: i32) {
    self.0.put_i32(val);
  }

  #[inline]
  pub fn put_i32_le(&mut self, val: i32) {
    self.0.put_i32_le(val);
  }

  #[inline]
  pub fn put_i64(&mut self, val: i64) {
    self.0.put_i64(val);
  }

  #[inline]
  pub fn put_i64_le(&mut self, val: i64) {
    self.0.put_i64_le(val);
  }

  #[inline]
  pub fn put_f32(&mut self, val: f32) {
    self.0.put_f32(val);
  }

  #[inline]
  pub fn put_f32_le(&mut self, val: f32) {
    self.0.put_f32_le(val);
  }

  #[inline]
  pub fn put_f64(&mut self, val: f64) {
    self.0.put_f64(val);
  }

  #[inline]
  pub fn put_f64_le(&mut self, val: f64) {
    self.0.put_f64_le(val);
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.0.put_u8(val as u8);
  }

  #[inline]
  pub fn put_bytes(&mut self, val: u8, cnt: usize) {
    self.0.put_bytes(val, cnt);
  }

  #[inline]
  pub fn clear(&mut self) {
    let mt = self.0[0];
    self.0.clear();
    self.0.put_u8(mt);
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub fn as_slice_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub(crate) fn freeze(self) -> Bytes {
    self.0.freeze()
  }
}

impl std::io::Write for Message {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.0.put_slice(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl Deref for Message {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl DerefMut for Message {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

impl AsRef<[u8]> for Message {
  fn as_ref(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl AsMut<[u8]> for Message {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

impl From<Message> for Bytes {
  fn from(msg: Message) -> Self {
    msg.0.freeze()
  }
}

/// An ID of a type of message that can be received
/// on network channels from other members.
///
/// The list of available message types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub(crate) enum MessageType {
  Ping = 0,
  IndirectPing = 1,
  AckResponse = 2,
  Suspect = 3,
  Alive = 4,
  Dead = 5,
  PushPull = 6,
  Compound = 7,
  /// User mesg, not handled by us
  User = 8,
  Compress = 9,
  Encrypt = 10,
  NackResponse = 11,
  HasCrc = 12,
  ErrorResponse = 13,
  /// HasLabel has a deliberately high value so that you can disambiguate
  /// it from the encryptionVersion header which is either 0/1 right now and
  /// also any of the existing [`MessageType`].
  HasLabel = 244,
}

impl MessageType {
  #[doc(hidden)]
  pub const SIZE: usize = core::mem::size_of::<Self>();
}

impl core::fmt::Display for MessageType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Ping => write!(f, "ping"),
      Self::IndirectPing => write!(f, "indirect ping"),
      Self::AckResponse => write!(f, "ack"),
      Self::Suspect => write!(f, "suspect"),
      Self::Alive => write!(f, "alive"),
      Self::Dead => write!(f, "dead"),
      Self::PushPull => write!(f, "push pull"),
      Self::Compound => write!(f, "compound"),
      Self::User => write!(f, "user"),
      Self::Compress => write!(f, "compress"),
      Self::Encrypt => write!(f, "encrypt"),
      Self::NackResponse => write!(f, "nack"),
      Self::HasCrc => write!(f, "crc"),
      Self::ErrorResponse => write!(f, "error"),
      Self::HasLabel => write!(f, "label"),
    }
  }
}

impl MessageType {
  /// Returns the str of the [`MessageType`].
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      MessageType::Ping => "ping",
      MessageType::IndirectPing => "indirect ping",
      MessageType::AckResponse => "ack response",
      MessageType::Suspect => "suspect",
      MessageType::Alive => "alive",
      MessageType::Dead => "dead",
      MessageType::PushPull => "push pull",
      MessageType::Compound => "compound",
      MessageType::User => "user",
      MessageType::Compress => "compress",
      MessageType::Encrypt => "encrypt",
      MessageType::NackResponse => "nack response",
      MessageType::HasCrc => "crc",
      MessageType::ErrorResponse => "error",
      MessageType::HasLabel => "label",
    }
  }

  #[inline]
  pub(crate) const fn as_err_str(&self) -> &'static str {
    match self {
      MessageType::Ping => "ping msg",
      MessageType::IndirectPing => "indirect ping msg",
      MessageType::AckResponse => "ack msg",
      MessageType::Suspect => "suspect msg",
      MessageType::Alive => "alive msg",
      MessageType::Dead => "dead msg",
      MessageType::PushPull => "push pull msg",
      MessageType::Compound => "compound msg",
      MessageType::User => "user msg",
      MessageType::Compress => "compress msg",
      MessageType::Encrypt => "encrypt msg",
      MessageType::NackResponse => "nack msg",
      MessageType::HasCrc => "crc msg",
      MessageType::ErrorResponse => "error msg",
      MessageType::HasLabel => "label msg",
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidMessageType(u8);

impl core::fmt::Display for InvalidMessageType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid message type: {}", self.0)
  }
}

impl std::error::Error for InvalidMessageType {}

impl TryFrom<u8> for MessageType {
  type Error = InvalidMessageType;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Ping),
      1 => Ok(Self::IndirectPing),
      2 => Ok(Self::AckResponse),
      3 => Ok(Self::Suspect),
      4 => Ok(Self::Alive),
      5 => Ok(Self::Dead),
      6 => Ok(Self::PushPull),
      7 => Ok(Self::Compound),
      8 => Ok(Self::User),
      9 => Ok(Self::Compress),
      10 => Ok(Self::Encrypt),
      11 => Ok(Self::NackResponse),
      12 => Ok(Self::HasCrc),
      13 => Ok(Self::ErrorResponse),
      244 => Ok(Self::HasLabel),
      _ => Err(InvalidMessageType(value)),
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InvalidNodeState(u8);

impl core::fmt::Display for InvalidNodeState {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid node state: {}", self.0)
  }
}

impl std::error::Error for InvalidNodeState {}
