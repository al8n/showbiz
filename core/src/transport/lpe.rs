use std::marker::PhantomData;

use transformable::Transformable;

use super::{Message, MessageTransformError, Wire};

/// A [`Message`] wrapper for [`Lpe`] wire
#[derive(Debug, Clone)]
pub struct LpeMessage<I, A>(Message<I, A>);

impl<I, A> LpeMessage<I, A> {
  /// Construct a new `LpeMessage` instance
  #[inline]
  pub const fn new(msg: Message<I, A>) -> Self {
    Self(msg)
  }

  /// Returns the inner message
  #[inline]
  pub fn into_inner(self) -> Message<I, A> {
    self.0
  }
}

impl<I, A> TryFrom<Message<I, A>> for LpeMessage<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = MessageTransformError<I, A>;

  #[inline]
  fn try_from(msg: Message<I, A>) -> Result<Self, Self::Error> {
    Ok(Self(msg))
  }
}

impl<I, A> TryFrom<LpeMessage<I, A>> for Message<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = MessageTransformError<I, A>;

  #[inline]
  fn try_from(msg: LpeMessage<I, A>) -> Result<Self, Self::Error> {
    Ok(msg.0)
  }
}

/// A length-prefixed encoding [`Wire`] implementation
pub struct Lpe<I, A>(PhantomData<(I, A)>);

impl<I, A> Default for Lpe<I, A> {
  #[inline]
  fn default() -> Self {
    Self(PhantomData)
  }
}

impl<I, A> Lpe<I, A> {
  /// Create a new `Lpe` instance
  #[inline]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I, A> Clone for Lpe<I, A> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for Lpe<I, A> {}

impl<I, A> core::fmt::Debug for Lpe<I, A> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Lpe").finish()
  }
}

impl<I, A> core::fmt::Display for Lpe<I, A> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("Lpe")
  }
}

impl<I, A> Wire for Lpe<I, A>
where
  I: Transformable + core::fmt::Debug + Clone,
  A: Transformable + core::fmt::Debug + Clone,
{
  type Error = MessageTransformError<I, A>;
  type Id = I;
  type Address = A;
  type Message = LpeMessage<I, A>;

  fn encoded_len(msg: &Self::Message) -> usize {
    msg.0.encoded_len()
  }

  fn encode_message(msg: Self::Message, dst: &mut [u8]) -> Result<usize, Self::Error> {
    msg.0.encode(dst)
  }

  fn decode_message(src: &[u8]) -> Result<(usize, Self::Message), Self::Error> {
    Message::decode(src).map(|(len, msg)| (len, LpeMessage::new(msg)))
  }

  async fn decode_message_from_reader(
    mut conn: impl futures::prelude::AsyncRead + Send + Unpin,
  ) -> std::io::Result<(usize, Self::Message)> {
    Message::decode_from_async_reader(&mut conn)
      .await
      .map(|(len, msg)| (len, LpeMessage::new(msg)))
  }
}
