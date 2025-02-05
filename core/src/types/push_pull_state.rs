use std::sync::Arc;

use super::*;

use bytes::Bytes;
use either::Either;
use nodecraft::{CheapClone, Node};

/// Push node state is the state push to the remote server.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushNodeState<I, A> {
  /// The id of the push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the id of the push node state")
    ),
    setter(attrs(doc = "Sets the id of the push node state (Builder pattern)"))
  )]
  id: I,
  /// The address of the push node state.
  #[viewit(
    getter(
      const,
      rename = "address",
      style = "ref",
      attrs(doc = "Returns the address of the push node state")
    ),
    setter(
      rename = "with_address",
      attrs(doc = "Sets the address of the push node state (Builder pattern)")
    )
  )]
  addr: A,
  /// Metadata from the delegate for this push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the meta of the push node state")
    ),
    setter(attrs(doc = "Sets the meta of the push node state (Builder pattern)"))
  )]
  meta: Meta,
  /// The incarnation of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the incarnation of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the incarnation of the push node state (Builder pattern)")
    )
  )]
  incarnation: u32,
  /// The state of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the state of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the state of the push node state (Builder pattern)")
    )
  )]
  state: State,
  /// The protocol version of the push node state is speaking.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the protocol version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the protocol version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  protocol_version: ProtocolVersion,
  /// The delegate version of the push node state is speaking.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the delegate version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the delegate version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  delegate_version: DelegateVersion,
}

const ID_TAG: u8 = 1;
const ADDR_TAG: u8 = 2;
const META_TAG: u8 = 3;
const META_BYTE: u8 = super::merge(WireType::LengthDelimited, META_TAG);
const INCARNATION_TAG: u8 = 4;
const INCARNATION_BYTE: u8 = super::merge(WireType::Varint, INCARNATION_TAG);
const STATE_TAG: u8 = 5;
const STATE_BYTE: u8 = super::merge(WireType::Byte, STATE_TAG);
const PROTOCOL_VERSION_TAG: u8 = 6;
const PROTOCOL_VERSION_BYTE: u8 = super::merge(WireType::Byte, PROTOCOL_VERSION_TAG);
const DELEGATE_VERSION_TAG: u8 = 7;
const DELEGATE_VERSION_BYTE: u8 = super::merge(WireType::Byte, DELEGATE_VERSION_TAG);

impl<I, A> PushNodeState<I, A>
where
  I: super::Data,
  A: super::Data,
{
  const fn id_byte() -> u8 {
    super::merge(I::WIRE_TYPE, ID_TAG)
  }

  const fn addr_byte() -> u8 {
    super::merge(A::WIRE_TYPE, ADDR_TAG)
  }

  /// Returns the encoded length of the push node state.
  pub fn encoded_len(&self) -> usize {
    let mut len = 1 + super::encoded_data_len(&self.id);
    len += 1 + super::encoded_data_len(&self.addr);
    if !self.meta().is_empty() {
      len += 1 + super::encoded_length_delimited_len(self.meta.len());
    }
    len += 1 + self.incarnation.encoded_len();
    len += 1 + 1; // state
    len += 1 + 1; // protocol version
    len += 1 + 1; // delegate version
    len
  }

  /// Encodes the push node state into the buffer.
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(
            length_delimited::InsufficientBuffer::with_information($offset as u64, $len as u64)
              .into(),
          );
        }
      };
    }

    let len = buf.len();
    bail!(self(1, len));
    buf[offset] = Self::id_byte();
    offset += 1;
    offset += super::encode_data(&self.id, &mut buf[offset..])
      .map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

    bail!(self(offset + 1, len));
    buf[offset] = Self::addr_byte();
    offset += 1;
    offset += super::encode_data(&self.addr, &mut buf[offset..])
      .map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

    if !self.meta().is_empty() {
      bail!(self(offset + 1, len));
      buf[offset] = META_BYTE;
      offset += 1;
      offset += super::encode_length_delimited(&self.meta, &mut buf[offset..])
        .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;
    }
    bail!(self(offset + 1, len));
    offset += self.incarnation.encode(&mut buf[offset..])?;

    bail!(self(offset + 6, len));
    buf[offset] = STATE_BYTE;
    offset += 1;
    buf[offset] = self.state.into();
    offset += 1;
    buf[offset] = PROTOCOL_VERSION_BYTE;
    offset += 1;
    buf[offset] = self.protocol_version.into();
    offset += 1;
    buf[offset] = DELEGATE_VERSION_BYTE;
    offset += 1;
    buf[offset] = self.delegate_version.into();
    offset += 1;
    Ok(offset)
  }

  /// Decodes the push node state from the buffer.
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError> {
    let mut offset = 0;
    let mut id = None;
    let mut addr = None;
    let mut meta = None;
    let mut incarnation = None;
    let mut state = None;
    let mut protocol_version = None;
    let mut delegate_version = None;

    while offset < src.len() {
      let b = src[offset];
      offset += 1;

      match b {
        b if b == Self::id_byte() => {
          let (readed, value) = super::decode_data::<I>(&src[offset..])?;
          offset += readed;
          id = Some(value);
        }
        b if b == Self::addr_byte() => {
          let (readed, value) = super::decode_data::<A>(&src[offset..])?;
          offset += readed;
          addr = Some(value);
        }
        META_BYTE => {
          let (readed, value) =
            super::decode_length_delimited(WireType::LengthDelimited, &src[offset..])?;
          let val = Meta::try_from(value).map_err(|e| DecodeError::new(e.to_string()))?;
          offset += readed;
          meta = Some(val);
        }
        INCARNATION_BYTE => {
          let (readed, value) = super::decode_varint::<u32>(WireType::Varint, &src[offset..])?;
          offset += readed;
          incarnation = Some(value);
        }
        STATE_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          let value = State::from(src[offset]);
          offset += 1;
          state = Some(value);
        }
        PROTOCOL_VERSION_TAG => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          let value = ProtocolVersion::from(src[offset]);
          offset += 1;
          protocol_version = Some(value);
        }
        DELEGATE_VERSION_TAG => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          let value = DelegateVersion::from(src[offset]);
          offset += 1;
          delegate_version = Some(value);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        id: id.ok_or_else(|| DecodeError::new("missing id"))?,
        addr: addr.ok_or_else(|| DecodeError::new("missing addr"))?,
        meta: meta.unwrap_or_default(),
        incarnation: incarnation.ok_or_else(|| DecodeError::new("missing incarnation"))?,
        state: state.ok_or_else(|| DecodeError::new("missing state"))?,
        protocol_version: protocol_version.unwrap_or_default(),
        delegate_version: delegate_version.unwrap_or_default(),
      },
    ))
  }
}

impl<I, A> PushNodeState<I, A> {
  /// Construct a new push node state with the given id, address and state.
  #[inline]
  pub const fn new(incarnation: u32, id: I, addr: A, state: State) -> Self {
    Self {
      id,
      addr,
      meta: Meta::empty(),
      incarnation,
      state,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Sets the id of the push node state
  #[inline]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the address of the push node state
  #[inline]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the meta of the push node state
  #[inline]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the incarnation of the push node state
  #[inline]
  pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the state of the push node state
  #[inline]
  pub fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
    self
  }

  /// Sets the protocol version of the push node state
  #[inline]
  pub fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the delegate version of the push node state
  #[inline]
  pub fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for PushNodeState<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
      meta: self.meta.clone(),
      incarnation: self.incarnation,
      state: self.state,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

impl<I: CheapClone, A: CheapClone> PushNodeState<I, A> {
  /// Returns a [`Node`] with the same id and address as this [`PushNodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

/// Push pull message.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PushPull<I, A> {
  /// Whether the push pull message is a join message.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns whether the push pull message is a join message")
    ),
    setter(
      const,
      attrs(doc = "Sets whether the push pull message is a join message (Builder pattern)")
    )
  )]
  join: bool,
  /// The states of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the states of the push pull message")
    ),
    setter(attrs(doc = "Sets the states of the push pull message (Builder pattern)"))
  )]
  states: Arc<[PushNodeState<I, A>]>,
  /// The user data of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the user data of the push pull message")
    ),
    setter(attrs(doc = "Sets the user data of the push pull message (Builder pattern)"))
  )]
  user_data: Bytes,
}

impl<I, A> Clone for PushPull<I, A> {
  fn clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> CheapClone for PushPull<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.cheap_clone(),
      user_data: self.user_data.clone(),
    }
  }
}

const JOIN_TAG: u8 = 1;
const JOIN_BYTE: u8 = super::merge(WireType::Varint, JOIN_TAG);
const STATES_TAG: u8 = 2;
const STATES_BYTE: u8 = super::merge(WireType::LengthDelimited, STATES_TAG);
const USER_DATA_TAG: u8 = 3;
const USER_DATA_BYTE: u8 = super::merge(WireType::LengthDelimited, USER_DATA_TAG);

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline]
  pub fn new(join: bool, states: TinyVec<PushNodeState<I, A>>) -> Self {
    Self {
      states: match TinyVec::into_either(states) {
        Either::Left(states) => Arc::from_iter(states),
        Either::Right(states) => Arc::from(states.into_vec()),
      },

      user_data: Bytes::new(),
      join,
    }
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline]
  pub fn into_components(self) -> (bool, Bytes, Arc<[PushNodeState<I, A>]>) {
    (self.join, self.user_data, self.states)
  }

  /// Returns the encoded length of the push pull message.
  #[inline]
  pub fn encoded_len(&self) -> usize
  where
    I: super::Data,
    A: super::Data,
  {
    let mut len = 1 + 1; // join
    for i in self.states.iter() {
      len += 1 + i.encoded_len();
    }
    let udl = self.user_data.len();
    if udl != 0 {
      len += 1 + super::encoded_length_delimited_len(udl);
    }
    len
  }

  /// Encodes the push pull message into the buffer.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>
  where
    I: super::Data,
    A: super::Data,
  {
    let mut offset = 0;
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(
            length_delimited::InsufficientBuffer::with_information($offset as u64, $len as u64)
              .into(),
          );
        }
      };
    }

    let len = buf.len();
    bail!(self(1, len));
    buf[offset] = JOIN_BYTE;
    offset += 1;
    buf[offset] = self.join as u8;
    offset += 1;

    for i in self.states.iter() {
      bail!(self(offset + 1, len));
      buf[offset] = STATES_BYTE;
      offset += 1;
      offset += i.encode(&mut buf[offset..])?;
    }

    let udl = self.user_data.len();
    if udl != 0 {
      bail!(self(offset + 1, len));
      buf[offset] = USER_DATA_BYTE;
      offset += 1;
      offset += super::encode_length_delimited(&self.user_data, &mut buf[offset..])?;
    }

    Ok(offset)
  }

  /// Decodes the push pull message from the buffer.
  #[inline]
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    I: super::Data,
    A: super::Data,
  {
    let mut offset = 0;
    let mut join = None;
    let mut states = TinyVec::new();
    let mut user_data = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        JOIN_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          let val = src[offset];
          offset += 1;
          join = Some(val);
        }
        STATES_BYTE => {
          let (readed, value) = PushNodeState::<I, A>::decode(&src[offset..])?;
          offset += readed;
          states.push(value);
        }
        USER_DATA_BYTE => {
          let (readed, value) =
            super::decode_length_delimited(WireType::LengthDelimited, &src[offset..])?;
          offset += readed;
          user_data = Some(Bytes::copy_from_slice(value));
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    let join = join.ok_or(DecodeError::new("missing join"))? != 0;
    let states = match states.into_either() {
      Either::Left(states) => Arc::from_iter(states),
      Either::Right(states) => Arc::from(states.into_vec()),
    };
    let user_data = user_data.unwrap_or_default();
    Ok((
      offset,
      Self {
        join,
        states,
        user_data,
      },
    ))
  }
}

#[cfg(feature = "arbitrary")]
const _: () = {
  use super::*;
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a, I, A> Arbitrary<'a> for PushNodeState<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self {
        id: I::arbitrary(u)?,
        addr: A::arbitrary(u)?,
        meta: Meta::arbitrary(u)?,
        incarnation: u.arbitrary()?,
        state: u.arbitrary()?,
        protocol_version: u.arbitrary()?,
        delegate_version: u.arbitrary()?,
      })
    }
  }

  impl<'a, I, A> Arbitrary<'a> for PushPull<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      let join = u.arbitrary()?;
      let states = u.arbitrary::<Vec<_>>()?;
      let user_data = Vec::<u8>::arbitrary(u)?.into();
      Ok(Self {
        join,
        states: Arc::from(states),
        user_data,
      })
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use super::*;
  use quickcheck::{Arbitrary, Gen};

  impl<I, A> Arbitrary for PushNodeState<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      Self {
        id: I::arbitrary(g),
        addr: A::arbitrary(g),
        meta: Meta::arbitrary(g),
        incarnation: u32::arbitrary(g),
        state: State::arbitrary(g),
        protocol_version: ProtocolVersion::arbitrary(g),
        delegate_version: DelegateVersion::arbitrary(g),
      }
    }
  }

  impl<I, A> Arbitrary for PushPull<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      let join = bool::arbitrary(g);
      let states = Vec::<PushNodeState<I, A>>::arbitrary(g);
      let user_data = Vec::<u8>::arbitrary(g).into();
      Self {
        join,
        states: Arc::from(states),
        user_data,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use arbitrary::{Arbitrary, Unstructured};

  use super::*;

  // #[test]
  // fn test_push_server_state() {
  //   for i in 0..100 {
  //     let state = PushNodeState::generate(i);
  //     let mut buf = vec![0; state.encoded_len()];
  //     let encoded_len = state.encode(&mut buf).unwrap();
  //     assert_eq!(encoded_len, state.encoded_len());
  //     let (readed, decoded) = PushNodeState::decode(&buf).unwrap();
  //     assert_eq!(readed, encoded_len);
  //     assert_eq!(decoded, state);
  //   }
  // }

  // #[test]
  // fn test_push_pull() {
  //   for i in 0..100 {
  //     let push_pull = PushPull::generate(i);
  //     let mut buf = vec![0; push_pull.encoded_len()];
  //     let encoded_len = push_pull.encode(&mut buf).unwrap();
  //     assert_eq!(encoded_len, push_pull.encoded_len());
  //     let (readed, decoded) = PushPull::decode(&buf).unwrap();
  //     assert_eq!(readed, encoded_len);
  //     assert_eq!(decoded, push_pull);
  //   }
  // }

  #[test]
  fn test_push_pull_clone_and_cheap_clone() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);

    let push_pull = PushPull::<String, SocketAddr>::arbitrary(&mut data).unwrap();
    let cloned = push_pull.clone();
    let cheap_cloned = push_pull.cheap_clone();
    assert_eq!(cloned, push_pull);
    assert_eq!(cheap_cloned, push_pull);
    let cloned1 = format!("{:?}", cloned);
    let cheap_cloned1 = format!("{:?}", cheap_cloned);
    assert_eq!(cloned1, cheap_cloned1);
  }

  #[test]
  fn test_push_node_state() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);

    let mut state = PushNodeState::<String, SocketAddr>::arbitrary(&mut data).unwrap();
    state.set_id("test".into());
    assert_eq!(state.id(), "test");
    state.set_address(SocketAddr::from(([127, 0, 0, 1], 8080)));
    assert_eq!(state.address(), &SocketAddr::from(([127, 0, 0, 1], 8080)));
    state.set_meta(Meta::try_from("test").unwrap());
    assert_eq!(state.meta(), &Meta::try_from("test").unwrap());
    state.set_incarnation(100);
    assert_eq!(state.incarnation(), 100);

    state.set_state(State::Alive);
    assert_eq!(state.state(), State::Alive);

    state.set_protocol_version(ProtocolVersion::V1);
    assert_eq!(state.protocol_version(), ProtocolVersion::V1);

    state.set_delegate_version(DelegateVersion::V1);
    assert_eq!(state.delegate_version(), DelegateVersion::V1);
  }
}
