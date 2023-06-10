use crate::showbiz::Showbiz;

use super::*;
use crate::delegate::Delegate;
use futures_util::{
  io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader},
  AsyncBufReadExt, AsyncWriteExt,
};

pin_project_lite::pin_project! {
  #[derive(Debug)]
  pub struct LabeledConnection<R> {
    label: Bytes,
    #[pin]
    pub(crate) conn: BufReader<R>,
  }
}

impl<R> LabeledConnection<R> {
  /// Returns the label if present.
  #[inline]
  pub const fn label(&self) -> &Bytes {
    &self.label
  }

  #[inline]
  pub fn set_label(&mut self, label: Bytes) {
    self.label = label;
  }
}

impl<R: AsyncRead> LabeledConnection<R> {
  #[inline]
  pub(crate) fn new(reader: BufReader<R>) -> Self {
    Self {
      label: Bytes::new(),
      conn: reader,
    }
  }

  #[inline]
  pub(crate) fn with_label(reader: BufReader<R>, label: Bytes) -> Self {
    Self {
      label,
      conn: reader,
    }
  }
}

impl<R: AsyncRead> AsyncRead for LabeledConnection<R> {
  fn poll_read(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<futures_util::io::Result<usize>> {
    self.project().conn.poll_read(cx, buf)
  }
}

impl<R: AsyncBufRead> AsyncBufRead for LabeledConnection<R> {
  fn poll_fill_buf(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<futures_util::io::Result<&[u8]>> {
    self.project().conn.poll_fill_buf(cx)
  }

  fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
    self.project().conn.consume(amt)
  }
}

impl<W: AsyncWrite> AsyncWrite for LabeledConnection<W> {
  fn poll_write(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &[u8],
  ) -> std::task::Poll<futures_util::io::Result<usize>> {
    self.project().conn.poll_write(cx, buf)
  }

  fn poll_flush(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<futures_util::io::Result<()>> {
    self.project().conn.poll_flush(cx)
  }

  fn poll_close(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<futures_util::io::Result<()>> {
    self.project().conn.poll_close(cx)
  }
}
