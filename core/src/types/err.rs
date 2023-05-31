use super::*;

#[viewit::viewit]
#[derive(Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: String,
}

impl<E: std::error::Error> From<E> for ErrorResponse {
  fn from(err: E) -> Self {
    Self {
      err: err.to_string(),
    }
  }
}

impl ErrorResponse {
  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic_len = if self.err.is_empty() {
      0
    } else {
      // err len + err + err tag
      encoded_u32_len(self.err.len() as u32) + self.err.len() + 1
    };
    basic_len + encoded_u32_len(basic_len as u32)
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, buf: &mut BytesMut) {
    encode_u32_to_buf(buf, self.encoded_len() as u32);
    buf.put_u8(1); // tag
    encode_u32_to_buf(buf, self.err.len() as u32);
    buf.put_slice(self.err.as_bytes());
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut required = 0;
    let mut this = Self { err: String::new() };
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          let len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated(
              MessageType::ErrorResponse.as_err_str(),
            ));
          }
          this.err = match String::from_utf8(buf.split_to(len).to_vec()) {
            Ok(s) => s,
            Err(e) => return Err(DecodeError::InvalidErrorResponse(e)),
          };
          required += 1;
        }
        _ => {}
      }
    }

    if required != 1 {
      return Err(DecodeError::Truncated(
        MessageType::ErrorResponse.as_err_str(),
      ));
    }
    Ok(this)
  }
}
