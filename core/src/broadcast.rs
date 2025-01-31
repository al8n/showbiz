use crate::{
  base::Memberlist,
  delegate::{Delegate, DelegateError},
  error::Error,
  transport::{Transport, Wire},
  types::{Message, TinyVec},
};
use async_channel::Sender;

use nodecraft::resolver::AddressResolver;

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
pub trait Broadcast: core::fmt::Debug + Send + Sync + 'static {
  /// The id type
  type Id: Clone + Eq + core::hash::Hash + core::fmt::Debug + core::fmt::Display;
  /// The message type
  type Message: Clone + core::fmt::Debug + Send + Sync + 'static;

  /// An optional extension of the Broadcast trait that
  /// gives each message a unique id and that is used to optimize
  fn id(&self) -> Option<&Self::Id>;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns the message
  fn message(&self) -> &Self::Message;

  /// Returns the encoded length of the message
  fn encoded_len(msg: &Self::Message) -> usize;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  fn finished(&self) -> impl std::future::Future<Output = ()> + Send;

  /// Indicates that each message is
  /// intrinsically unique and there is no need to scan the broadcast queue for
  /// duplicates.
  ///
  /// You should ensure that `invalidates` always returns false if implementing
  /// this.
  fn is_unique(&self) -> bool {
    false
  }
}

#[viewit::viewit]
pub(crate) struct MemberlistBroadcast<W: Wire> {
  node: W::Id,
  msg: W::Message,
  notify: Option<async_channel::Sender<()>>,
}

impl<W> core::fmt::Debug for MemberlistBroadcast<W>
where
  W: Wire,
  W::Id: core::fmt::Debug,
  W::Address: core::fmt::Debug,
  W::Message: core::fmt::Debug,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("node", &self.node)
      .field("msg", &self.msg)
      .finish()
  }
}

impl<W> Broadcast for MemberlistBroadcast<W>
where
  W: Wire,
  W::Id: core::fmt::Debug + core::fmt::Display + core::hash::Hash + Clone + PartialEq + Eq,
  W::Address: core::fmt::Debug,
  W::Message: core::fmt::Debug + Clone,
{
  type Id = W::Id;
  type Message = W::Message;

  fn id(&self) -> Option<&Self::Id> {
    Some(&self.node)
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node.eq(&other.node)
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    W::encoded_len(msg)
  }

  async fn finished(&self) {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!("memberlist: broadcast failed to notify: {}", e);
      }
    }
  }

  fn is_unique(&self) -> bool {
    false
  }
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::ResolvedAddress,
    Wire = T::Wire,
  >,
  T: Transport,
{
  #[inline]
  pub(crate) async fn broadcast_notify(
    &self,
    node: T::Id,
    msg: <T::Wire as Wire>::Message,
    notify_tx: Option<Sender<()>>,
  ) {
    let _ = self.queue_broadcast(node, msg, notify_tx).await;
  }

  #[inline]
  pub(crate) async fn broadcast(&self, node: T::Id, msg: <T::Wire as Wire>::Message) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  async fn queue_broadcast(
    &self,
    node: T::Id,
    msg: <T::Wire as Wire>::Message,
    notify_tx: Option<Sender<()>>,
  ) {
    self
      .inner
      .broadcast
      .queue_broadcast(MemberlistBroadcast {
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
    to_send: TinyVec<<T::Wire as Wire>::Message>,
    overhead: usize,
    limit: usize,
  ) -> Result<TinyVec<<T::Wire as Wire>::Message>, Error<T, D>> {
    // Get memberlist messages first
    let mut to_send = self
      .inner
      .broadcast
      .get_broadcast_with_prepend(to_send, overhead, limit)
      .await;

    // Check if the user has anything to broadcast
    if let Some(delegate) = &self.delegate {
      // Determine the bytes used already
      let mut bytes_used = 0;
      for msg in to_send.iter() {
        bytes_used += <T::Wire as Wire>::encoded_len(msg) + overhead;
      }

      // Check space remaining for user messages
      let avail = limit.saturating_sub(bytes_used);
      if avail > overhead {
        delegate
          .broadcast_messages(overhead, avail, |b| {
            let msg = <<T::Wire as Wire>::Message as TryFrom<_>>::try_from(Message::<
              T::Id,
              <T::Resolver as AddressResolver>::ResolvedAddress,
            >::UserData(
              b.clone()
            ))?;
            let len = <T::Wire as Wire>::encoded_len(&msg);
            to_send.push(msg);
            Ok(len)
          })
          .await
          .map_err(|e| Error::Delegate(DelegateError::NodeDelegate(e)))?;
      }
    }

    Ok(to_send)
  }
}
