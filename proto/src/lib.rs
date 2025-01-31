use memberlist_core::{
  futures::AsyncRead,
  transport::{Transformable, Wire},
  types::Message,
};

mod types;

#[derive(Debug)]
pub enum Error {
  Decode(prost::DecodeError),
  Encode(prost::EncodeError),
}

impl From<prost::DecodeError> for Error {
  fn from(e: prost::DecodeError) -> Self {
    Self::Decode(e)
  }
}

impl From<prost::EncodeError> for Error {
  fn from(e: prost::EncodeError) -> Self {
    Self::Encode(e)
  }
}

impl core::fmt::Display for Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Decode(e) => write!(f, "{e}"),
      Self::Encode(e) => write!(f, "{e}"),
    }
  }
}

impl core::error::Error for Error {}

pub struct Protobuf<I, A> {
  _phantom: core::marker::PhantomData<(I, A)>,
}

impl<I, A> Wire for Protobuf<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = Error;

  type Id = I;

  type Address = A;

  type Message = types::Message;

  fn encoded_len(msg: &Self::Message) -> usize {
    todo!()
  }

  fn encode_message(msg: Self::Message, dst: &mut [u8]) -> Result<usize, Self::Error> {
    todo!()
  }

  fn decode_message(src: &[u8]) -> Result<(usize, Self::Message), Self::Error> {
    todo!()
  }

  async fn decode_message_from_reader(
    conn: impl AsyncRead + Send + Unpin,
  ) -> std::io::Result<(usize, Self::Message)> {
    todo!()
  }
}
