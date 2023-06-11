use super::CompressionAlgo;

pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

const LZW_LIT_WIDTH: u8 = 8;

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
  #[error("{0}")]
  LZW(#[from] weezl::LzwError),
}

#[inline]
pub(crate) fn decompress_buffer(
  cmp: CompressionAlgo,
  data: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .decode(data)
      .map_err(CompressionError::LZW),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn compress_payload(
  cmp: CompressionAlgo,
  inp: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .encode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn decompress_payload(
  cmp: CompressionAlgo,
  inp: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match cmp {
    CompressionAlgo::LZW => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .decode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}
