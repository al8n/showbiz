use std::sync::Arc;

use bytes::Bytes;
use memberlist_types::{Meta, TinyVec};
use nodecraft::{CheapClone, Id};

use crate::{
  transport::Wire,
  types::{NodeState, SmallVec},
};

#[cfg(any(test, feature = "test"))]
#[doc(hidden)]
pub mod mock;

mod alive;
pub use alive::*;

mod conflict;
pub use conflict::*;

mod composite;
pub use composite::*;

mod event;
pub use event::*;

mod node;
pub use node::*;

mod merge;
pub use merge::*;

mod ping;
pub use ping::*;

/// Error trait for [`Delegate`]
pub enum DelegateError<D: Delegate> {
  /// [`AliveDelegate`] error
  AliveDelegate(<D as AliveDelegate>::Error),
  /// [`MergeDelegate`] error
  MergeDelegate(<D as MergeDelegate>::Error),
  /// [`NodeDelegate`] error
  NodeDelegate(<<D as NodeDelegate>::Wire as Wire>::Error),
}

impl<D: Delegate> core::fmt::Debug for DelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::AliveDelegate(err) => write!(f, "{err:?}"),
      Self::MergeDelegate(err) => write!(f, "{err:?}"),
      Self::NodeDelegate(err) => write!(f, "{err:?}"),
    }
  }
}

impl<D: Delegate> core::fmt::Display for DelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::AliveDelegate(err) => write!(f, "{err}"),
      Self::MergeDelegate(err) => write!(f, "{err}"),
      Self::NodeDelegate(err) => write!(f, "{err}"),
    }
  }
}

impl<D: Delegate> std::error::Error for DelegateError<D> {}

impl<D: Delegate> DelegateError<D> {
  /// Create a delegate error from an alive delegate error.
  #[inline]
  pub const fn alive(err: <D as AliveDelegate>::Error) -> Self {
    Self::AliveDelegate(err)
  }

  /// Create a delegate error from a merge delegate error.
  #[inline]
  pub const fn merge(err: <D as MergeDelegate>::Error) -> Self {
    Self::MergeDelegate(err)
  }
}

/// [`Delegate`] is the trait that clients must implement if they want to hook
/// into the gossip layer of [`Memberlist`](crate::Memberlist). All the methods must be thread-safe,
/// as they can and generally will be called concurrently.
pub trait Delegate:
  NodeDelegate
  + PingDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + EventDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + ConflictDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + AliveDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + MergeDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
{
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;
}

/// Error type for [`VoidDelegate`].
#[derive(Debug, Copy, Clone)]
pub struct VoidDelegateError;

impl std::fmt::Display for VoidDelegateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "void delegate error")
  }
}

impl std::error::Error for VoidDelegateError {}

/// Void delegate
#[derive(Debug, Copy, Clone)]
pub struct VoidDelegate<I, A, W>(core::marker::PhantomData<(I, A, W)>);

impl<I, A, W> Default for VoidDelegate<I, A, W> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, W> VoidDelegate<I, A, W> {
  /// Creates a new [`VoidDelegate`].
  #[inline]
  pub const fn new() -> Self {
    Self(core::marker::PhantomData)
  }
}

impl<I, A, W> AliveDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Send + Sync + 'static,
{
  type Error = VoidDelegateError;
  type Id = I;
  type Address = A;

  async fn notify_alive(
    &self,
    _peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl<I, A, W> MergeDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Send + Sync + 'static,
{
  type Error = VoidDelegateError;
  type Id = I;
  type Address = A;

  async fn notify_merge(
    &self,
    _peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl<I, A, W> ConflictDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn notify_conflict(
    &self,
    _existing: Arc<NodeState<Self::Id, Self::Address>>,
    _other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
  }
}

impl<I, A, W> PingDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn ack_payload(&self) -> Bytes {
    Bytes::new()
  }

  async fn notify_ping_complete(
    &self,
    _node: Arc<NodeState<Self::Id, Self::Address>>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) {
  }

  fn disable_promised_pings(&self, _target: &Self::Id) -> bool {
    false
  }
}

impl<I, A, W> EventDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn notify_join(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}

  async fn notify_leave(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}

  async fn notify_update(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}
}

impl<I, A, W> NodeDelegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Wire,
{
  type Wire = W;

  async fn node_meta(&self, _limit: usize) -> Meta {
    Meta::empty()
  }

  async fn notify_message(&self, _msg: Bytes) {}

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> Result<(), <Self::Wire as Wire>::Error>
  where
    F: FnMut(Bytes) -> Result<usize, <Self::Wire as Wire>::Error> + Send,
  {
    Ok(())
  }

  async fn local_state(&self, _join: bool) -> Bytes {
    Bytes::new()
  }

  async fn merge_remote_state(&self, _buf: Bytes, _join: bool) {}
}

impl<I, A, W> Delegate for VoidDelegate<I, A, W>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
  W: Wire,
{
  type Id = I;
  type Address = A;
}
