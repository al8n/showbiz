use crate::{
  delegate::Delegate,
  error::Error,
  network::USER_MSG_OVERHEAD,
  showbiz::Showbiz,
  transport::Transport,
  types::{Message, Name},
};
use async_channel::Sender;

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait Broadcast: Send + Sync + 'static {
  /// Error type
  type Error: std::error::Error + Send + Sync + 'static;

  /// Returns the name of the broadcast, if any
  fn name(&self) -> &Name;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns bytes form of the message
  fn message(&self) -> &Message;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  #[cfg(not(feature = "async"))]
  fn finished(&self) -> Result<(), Self::Error>;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  #[cfg(feature = "async")]
  async fn finished(&self) -> Result<(), Self::Error>;

  /// Indicates that each message is
  /// intrinsically unique and there is no need to scan the broadcast queue for
  /// duplicates.
  fn is_unique(&self) -> bool;
}

pub(crate) struct ShowbizBroadcast {
  node: Name,
  msg: Message,
  #[cfg(feature = "async")]
  notify: Option<async_channel::Sender<()>>,
  #[cfg(not(feature = "async"))]
  notify: Option<crossbeam_channel::Sender<()>>,
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl Broadcast for ShowbizBroadcast {
  #[cfg(feature = "async")]
  type Error = async_channel::SendError<()>;

  #[cfg(not(feature = "async"))]
  type Error = crossbeam_channel::SendError<()>;

  fn name(&self) -> &Name {
    &self.node
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node == other.node
  }

  fn message(&self) -> &Message {
    &self.msg
  }

  #[cfg(feature = "async")]
  async fn finished(&self) -> Result<(), Self::Error> {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!(target = "showbiz", "failed to notify: {}", e);
        return Err(e);
      }
    }
    Ok(())
  }

  #[cfg(not(feature = "async"))]
  fn finished(&self) -> Result<(), Self::Error> {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()) {
        tracing::error!(target = "showbiz", "failed to notify: {}", e);
        return Err(e);
      }
    }
    Ok(())
  }

  fn is_unique(&self) -> bool {
    false
  }
}

#[cfg(feature = "async")]
impl<T: Transport, D: Delegate> Showbiz<T, D> {
  #[inline]
  pub(crate) async fn broadcast_notify(&self, node: Name, msg: Message, notify_tx: Sender<()>) {
    let _ = self.queue_broadcast(node, msg, Some(notify_tx)).await;
  }

  #[inline]
  pub(crate) async fn broadcast(&self, node: Name, msg: Message) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  pub(crate) async fn queue_broadcast(
    &self,
    node: Name,
    msg: Message,
    notify_tx: Option<Sender<()>>,
  ) -> Result<(), async_channel::SendError<()>> {
    self
      .inner
      .broadcast
      .queue_broadcast(ShowbizBroadcast {
        node,
        msg,
        notify: notify_tx,
      })
      .await
  }

  /// Used to return a slice of broadcasts to send up to
  /// a maximum byte size, while imposing a per-broadcast overhead. This is used
  /// to fill a UDP packet with piggybacked data
  #[inline]
  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    to_send: Vec<Message>,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Error<T, D>> {
    // Get memberlist messages first
    let mut to_send = self
      .inner
      .broadcast
      .get_broadcast_with_prepend(to_send, overhead, limit)
      .await?;

    // Check if the user has anything to broadcast
    if let Some(delegate) = &self.inner.delegate {
      // Determine the bytes used already
      let mut bytes_used = 0;
      for msg in &to_send {
        bytes_used += msg.len() + overhead;
      }

      // Check space remaining for user messages
      let avail = limit.saturating_sub(bytes_used);
      if avail > overhead + USER_MSG_OVERHEAD {
        to_send.extend(
          delegate
            .get_broadcasts(overhead + USER_MSG_OVERHEAD, avail)
            .await
            .map_err(Error::delegate)?,
        );
      }
    }

    Ok(to_send)
  }
}
