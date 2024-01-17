use std::sync::atomic::Ordering;

use crate::{
  delegate::Delegate,
  error::Error,
  // security::{append_bytes, EncryptionAlgo, SecretKey, SecretKeyring, SecurityError},
  transport::{PromisedStream, TimeoutableStream},
  // types::MessageType,
};

use super::*;
use agnostic::Runtime;
use bytes::Buf;
use futures::{future::FutureExt, Future, Stream};
use nodecraft::{resolver::AddressResolver, Node};

mod packet;
mod stream;

// #[cfg(any(test, feature = "test"))]
// pub(crate) mod tests;
// #[cfg(any(test, feature = "test"))]
// pub use tests::*;

impl<D, T> Showbiz<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &<T::Resolver as AddressResolver>::ResolvedAddress,
    ping: Ping<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    deadline: Duration,
  ) -> Result<bool, Error<T, D>> {
    let mut conn: T::PromisedStream =
      match self.inner.transport.dial_timeout(target, deadline).await {
        Ok(conn) => conn,
        Err(_) => {
          // If the node is actually dead we expect this to fail, so we
          // shouldn't spam the logs with it. After this point, errors
          // with the connection are real, unexpected errors and should
          // get propagated up.
          return Ok(false);
        }
      };
    if deadline != Duration::ZERO {
      conn.set_timeout(Some(deadline));
    }

    let ping_seq_no = ping.seq_no;
    conn
      .send_message(target, ping.into())
      .await
      .map_err(Error::transport)?;

    let msg: Message<_, _> = conn.read_message().await.map_err(Error::transport)?;
    let kind = msg.kind();
    if let Some(ack) = msg.try_unwrap_ack_response() {
      if ack.seq_no != ping_seq_no {
        return Err(Error::sequence_number_mismatch(ping_seq_no, ack.seq_no));
      }

      Ok(true)
    } else {
      Err(Error::UnexpectedMessage {
        expected: "AckResponse",
        got: kind,
      })
    }
  }

  /// Used to initiate a push/pull over a stream with a
  /// remote host.
  pub(crate) async fn send_and_receive_state<'a>(
    &'a self,
    node: &'a Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    join: bool,
  ) -> Result<PushPull<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>> {
    // Attempt to connect
    let mut conn = self
      .inner
      .transport
      .dial_timeout(node.address(), self.inner.opts.timeout)
      .await
      .map_err(Error::transport)?;
    tracing::debug!(target:  "showbiz", local_addr = %self.inner.id, peer_addr = %node, "initiating push/pull sync");

    #[cfg(feature = "metrics")]
    {
      incr_tcp_connect_counter(self.inner.opts.metric_labels.iter());
    }

    // Send our state
    self
      .send_local_state(&mut conn, node.address(), join)
      .await?;

    conn.set_timeout(if self.inner.opts.timeout == Duration::ZERO {
      None
    } else {
      Some(self.inner.opts.timeout)
    });

    match conn.read_message().await.map_err(Error::transport)? {
      Message::ErrorResponse(err) => Err(Error::remote(err)),
      Message::PushPull(pp) => Ok(pp),
      msg => Err(Error::unexpected_message("PushPull", msg.kind())),
    }

    // let encryption_enabled = self.encryption_enabled();
    // let (h, data) = Self::read_stream(
    //   &mut conn,
    //   self.inner.opts.label.clone(),
    //   encryption_enabled,
    //   self.inner.opts.secret_keyring.clone(),
    //   &self.inner.opts,
    //   #[cfg(feature = "metrics")]
    //   &self.inner.opts.metric_labels,
    // )
    // .await?;

    // if h.meta.ty == MessageType::ErrorResponse {
    //   let err = match ErrorResponse::decode_archived(&data) {
    //     Ok((_, err)) => err,
    //     Err(e) => return Err(TransportError::Decode(e).into()),
    //   };
    //   return Err(Error::Peer(err.err.to_string()));
    // }

    // // Quit if not push/pull
    // if h.meta.ty != MessageType::PushPull {
    //   return Err(Error::Transport(TransportError::Decode(
    //     DecodeError::MismatchMessageType {
    //       expected: MessageType::PushPull.as_str(),
    //       got: h.meta.ty.as_str(),
    //     },
    //   )));
    // }

    // Ok(data)
  }

  // fn encrypt_local_state(
  //   primary_key: SecretKey,
  //   keyring: &SecretKeyring,
  //   msg: &[u8],
  //   label: &Label,
  //   algo: EncryptionAlgo,
  // ) -> Result<Bytes, Error<T, D>> {
  //   let mut buf = algo.header(msg.len());

  //   // Authenticated Data is:
  //   //
  //   //   [messageType; u8] [marker; u8] [encryptionAlgo; u8] [reserved1; u8]
  //   //   [messageLength; u32] [stream_label; optional]
  //   //
  //   let mut ciphertext = buf.split_off(ENCODE_HEADER_SIZE);
  //   if label.is_empty() {
  //     // Write the encrypted cipher text to the buffer
  //     keyring
  //       .encrypt_payload(primary_key, algo, msg, &buf, &mut ciphertext)
  //       .map(|_| {
  //         buf.unsplit(ciphertext);
  //         buf.freeze()
  //       })
  //       .map_err(From::from)
  //   } else {
  //     let data_bytes = append_bytes(&buf, label.as_bytes());
  //     // Write the encrypted cipher text to the buffer
  //     keyring
  //       .encrypt_payload(primary_key, algo, msg, &data_bytes, &mut ciphertext)
  //       .map(|_| {
  //         buf.unsplit(ciphertext);
  //         buf.freeze()
  //       })
  //       .map_err(From::from)
  //   }
  // }

  // fn decrypt_remote_state(
  //   stream_label: &Label,
  //   mut buf: BytesMut,
  //   keyring: &SecretKeyring,
  // ) -> Result<Bytes, Error<T, D>> {
  //   // Decrypt the cipherText with some authenticated data
  //   //
  //   // Authenticated Data is:
  //   //
  //   //   [messageType; u8] [marker; u8] [encryptionAlgo; u8] [reserved1; u8]
  //   //   [messageLength; u32] [stream_label; optional]
  //   //

  //   let mut ciphertext = buf.split_off(ENCODE_HEADER_SIZE);
  //   if stream_label.is_empty() {
  //     // Decrypt the payload
  //     keyring
  //       .decrypt_payload(&mut ciphertext, &buf)
  //       .map(|_| ciphertext.freeze())
  //       .map_err(From::from)
  //   } else {
  //     let data_bytes = append_bytes(&buf, stream_label.as_bytes());
  //     // Decrypt the payload
  //     keyring
  //       .decrypt_payload(&mut ciphertext, data_bytes.as_ref())
  //       .map(|_| ciphertext.freeze())
  //       .map_err(From::from)
  //   }
  // }
}
