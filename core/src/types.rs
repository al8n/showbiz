mod message;
pub use message::*;

mod ack;
pub use ack::*;

mod alive;
pub use alive::*;

mod bad_state;
pub use bad_state::*;

mod err;
pub use err::*;

mod ping;
pub use ping::*;

mod push_pull_state;
pub use push_pull_state::*;

use crate::{DelegateVersion, ProtocolVersion};

mod label;

mod packet;
pub use packet::*;

mod server;
pub use server::*;

#[test]
fn test_() {}

// const DEFAULT_ENCODE_PREALLOCATE_SIZE: usize = 128;
// pub(crate) const ENCODE_META_SIZE: usize =
//   MessageType::SIZE + ProtocolVersion::SIZE + DelegateVersion::SIZE + 1;
// pub(crate) const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();
// pub(crate) const MAX_MESSAGE_SIZE: usize = core::mem::size_of::<u32>();
// pub(crate) const ENCODE_HEADER_SIZE: usize = ENCODE_META_SIZE + MAX_MESSAGE_SIZE; // message length

// #[derive(Debug, Clone, Copy)]
// pub struct DecodeU32Error;

// impl core::fmt::Display for DecodeU32Error {
//   fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//     write!(f, "invalid length")
//   }
// }

// impl std::error::Error for DecodeU32Error {}

// #[derive(Debug, thiserror::Error)]
// pub enum DecodeError {
//   #[error("truncated {0}")]
//   Truncated(&'static str),
//   #[error("corrupted message")]
//   Corrupted,
//   #[error("checksum mismatch")]
//   ChecksumMismatch,
//   #[error("unknown mark bit {0}")]
//   UnknownMarkBit(u8),
//   #[error("fail to decode length {0}")]
//   Length(DecodeU32Error),
//   #[error("invalid ip addr length {0}")]
//   InvalidIpAddrLength(usize),

//   #[error("{0}")]
//   Decode(#[from] SharedDeserializeMapError),
//   #[error("compression error {0}")]
//   Compress(#[from] CompressError),
//   #[error("decompress error {0}")]
//   Decompress(#[from] DecompressError),
//   // #[error("{0}")]
//   // CheckTypeError(CheckTypeError<T::Archived, DefaultValidator<'a>>),
//   // #[error("invalid name {0}")]
//   // InvalidName(#[from] InvalidName),
//   #[error("invalid string {0}")]
//   InvalidErrorResponse(std::string::FromUtf8Error),
//   #[error("invalid size {0}")]
//   InvalidMessageSize(#[from] DecodeU32Error),
//   // #[error("{0}")]
//   // InvalidServerState(#[from] InvalidServerState),
//   #[error("{0}")]
//   InvalidProtocolVersion(#[from] InvalidProtocolVersion),
//   #[error("{0}")]
//   InvalidDelegateVersion(#[from] InvalidDelegateVersion),
//   #[error("{0}")]
//   InvalidMessageType(#[from] InvalidMessageType),
//   #[error("{0}")]
//   InvalidCompressionAlgo(#[from] InvalidCompressionAlgo),
//   #[error("{0}")]
//   InvalidLabel(#[from] InvalidLabel),
//   #[error("failed to read full push node state ({0} / {1})")]
//   FailReadRemoteState(usize, usize),
//   #[error("failed to read full user state ({0} / {1})")]
//   FailReadUserState(usize, usize),
//   #[error("mismatch message type, expected {expected}, got {got}")]
//   MismatchMessageType {
//     expected: &'static str,
//     got: &'static str,
//   },
//   #[error("sequence number from ack ({ack}) doesn't match ping ({ping})")]
//   MismatchSequenceNumber { ack: u32, ping: u32 },
//   #[error("check bytes error for type {0}")]
//   CheckBytesError(&'static str),
// }

// #[derive(Debug, thiserror::Error)]
// pub enum EncodeError {
//   #[error("{0}")]
//   InvalidLabel(#[from] InvalidLabel),
//   #[error("{0}")]
//   Compress(#[from] CompressError),
// }

// pub(crate) trait Type: Sized + Archive {
//   const PREALLOCATE: usize;

//   fn encode(&self) -> Message;

//   fn decode_archived<'a>(src: &'a [u8]) -> Result<(EncodeHeader, &'a Self::Archived), DecodeError>
//   where
//     <Self as Archive>::Archived: CheckBytes<DefaultValidator<'a>>,
//   {
//     let mt = src[0].try_into()?;
//     let marker = src[1];
//     let msgs = src[2];
//     let r1 = src[3];
//     let len = u32::from_be_bytes(
//       src[ENCODE_META_SIZE..ENCODE_HEADER_SIZE]
//         .try_into()
//         .unwrap(),
//     );
//     if marker == MessageType::HasCrc as u8 {
//       if len < CHECKSUM_SIZE as u32 {
//         return Err(DecodeError::Corrupted);
//       }
//       let crc = u32::from_be_bytes(
//         src[len as usize..len as usize + CHECKSUM_SIZE]
//           .try_into()
//           .unwrap(),
//       );
//       if crc != crc32fast::hash(&src[ENCODE_HEADER_SIZE..len as usize - CHECKSUM_SIZE]) {
//         return Err(DecodeError::ChecksumMismatch);
//       }
//     }

//     rkyv::check_archived_root::<Self>(&src[ENCODE_HEADER_SIZE..ENCODE_HEADER_SIZE + len as usize])
//       .map(|a| {
//         (
//           EncodeHeader {
//             meta: EncodeMeta {
//               ty: mt,
//               marker,
//               msgs,
//               r1,
//             },
//             len,
//           },
//           a,
//         )
//       })
//       .map_err(|_e| DecodeError::CheckBytesError(std::any::type_name::<Self>()))
//   }

//   fn from_bytes<'a>(src: &'a [u8]) -> Result<&'a Self::Archived, DecodeError>
//   where
//     <Self as Archive>::Archived: CheckBytes<DefaultValidator<'a>>,
//   {
//     rkyv::check_archived_root::<Self>(src)
//       .map_err(|_| DecodeError::CheckBytesError(std::any::type_name::<Self>()))
//   }

//   fn decode<'a>(src: &'a [u8]) -> Result<(EncodeHeader, Self), DecodeError>
//   where
//     Self: Archive,
//     Self::Archived: 'a + CheckBytes<DefaultValidator<'a>> + Deserialize<Self, SharedDeserializeMap>,
//   {
//     Self::decode_archived(src).and_then(|(h, archived)| {
//       archived
//         .deserialize(&mut SharedDeserializeMap::new())
//         .map(|v| (h, v))
//         .map_err(From::from)
//     })
//   }
// }

// fn encode<T, const N: usize>(ty: MessageType, msg: &T) -> Message
// where
//   T: Serialize<CompositeSerializer<Message, N>>,
// {
//   let mut ser = MessageSerializer::<N>::with_preallocated_size();
//   ser
//     .write(&[
//       ty as u8, 0, 0, 0, 0, 0, 0, 0, // len
//     ])
//     .unwrap();
//   ser
//     .serialize_value(msg)
//     .map(|_| {
//       let mut data = ser.into_writter();
//       data.write_message_len();
//       data
//     })
//     .unwrap()
// }

// #[viewit::viewit]
// #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
// pub(crate) struct EncodeMeta {
//   ty: MessageType,
//   // checksum byte, value 244 shows that this message need to be verified by checksum
//   marker: u8,
//   // compound byte, if messsage ty is Compound, this one will represent how many messages
//   msgs: u8,
//   // reserved byte for message specific
//   r1: u8,
// }

// #[viewit::viewit]
// #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
// pub(crate) struct EncodeHeader {
//   meta: EncodeMeta,
//   len: u32,
// }

// impl EncodeHeader {
//   #[inline]
//   pub(crate) const fn to_array(self) -> [u8; ENCODE_HEADER_SIZE] {
//     let len = self.len.to_be_bytes();
//     [
//       self.meta.ty as u8,
//       self.meta.marker,
//       self.meta.msgs,
//       self.meta.r1,
//       len[0],
//       len[1],
//       len[2],
//       len[3],
//     ]
//   }

//   #[inline]
//   pub(crate) fn from_bytes(src: &[u8]) -> Result<Self, DecodeError> {
//     Ok(Self {
//       meta: EncodeMeta {
//         ty: src[0].try_into()?,
//         marker: src[1],
//         msgs: src[2],
//         r1: src[3],
//       },
//       len: u32::from_be_bytes([src[4], src[5], src[6], src[7]]),
//     })
//   }
// }
