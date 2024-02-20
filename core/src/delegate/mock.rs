use futures::lock::Mutex;
use nodecraft::Address;
use std::{
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
  time::Duration,
};

use super::*;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum MockDelegateError {
  #[error("custom merge cancelled")]
  CustomMergeCancelled,
  #[error("custom alive cancelled")]
  CustomAliveCancelled,
}

#[derive(Debug, Eq, PartialEq)]
pub enum MockDelegateType {
  None,
  CancelMerge,
  CancelAlive,
  NotifyConflict,
  Ping,
  UserData,
}

struct MockDelegateInner<I, A> {
  meta: Bytes,
  msgs: Vec<Bytes>,
  broadcasts: Vec<Bytes>,
  state: Bytes,
  remote_state: Bytes,
  conflict_existing: Option<Arc<NodeState<I, A>>>,
  conflict_other: Option<Arc<NodeState<I, A>>>,
  ping_other: Option<Arc<NodeState<I, A>>>,
  ping_rtt: Duration,
  ping_payload: Bytes,
}

impl<I, A> Default for MockDelegateInner<I, A> {
  fn default() -> Self {
    Self {
      meta: Bytes::new(),
      msgs: vec![],
      broadcasts: vec![],
      state: Bytes::new(),
      remote_state: Bytes::new(),
      conflict_existing: None,
      conflict_other: None,
      ping_other: None,
      ping_rtt: Duration::from_secs(0),
      ping_payload: Bytes::new(),
    }
  }
}

pub struct MockDelegate<I, A> {
  inner: Arc<Mutex<MockDelegateInner<I, A>>>,
  invoked: Arc<AtomicBool>,
  ty: MockDelegateType,
  ignore: Option<I>,
  count: Arc<AtomicUsize>,
}

impl<I, A> Default for MockDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> MockDelegate<I, A> {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::None,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn cancel_merge() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelMerge,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn is_invoked(&self) -> bool {
    self.invoked.load(Ordering::SeqCst)
  }

  pub fn cancel_alive(ignore: I) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelAlive,
      ignore: Some(ignore),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn notify_conflict() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn ping() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn user_data() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::UserData,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn count(&self) -> usize {
    self.count.load(Ordering::SeqCst)
  }
}

impl<I, A> MockDelegate<I, A> {
  pub async fn set_meta(&self, meta: Bytes) {
    self.inner.lock().await.meta = meta;
  }

  pub async fn set_state(&self, state: Bytes) {
    self.inner.lock().await.state = state;
  }

  pub async fn set_broadcasts(&self, broadcasts: Vec<Bytes>) {
    self.inner.lock().await.broadcasts = broadcasts;
  }

  pub async fn get_remote_state(&self) -> Bytes {
    self.inner.lock().await.remote_state.clone()
  }

  pub async fn get_messages(&self) -> Vec<Bytes> {
    let mut mu = self.inner.lock().await;
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.msgs);
    out
  }

  pub async fn get_contents(&self) -> Option<(Arc<NodeState<I, A>>, Duration, Bytes)> {
    if self.ty == MockDelegateType::Ping {
      let mut mu = self.inner.lock().await;
      let other = mu.ping_other.take()?;
      let rtt = mu.ping_rtt;
      let payload = mu.ping_payload.clone();
      Some((other, rtt, payload))
    } else {
      None
    }
  }
}

impl<I: Id, A: Address> Delegate for MockDelegate<I, A> {
  type Error = MockDelegateError;
  type Address = A;
  type Id = I;

  async fn node_meta(&self, _limit: usize) -> Bytes {
    self.inner.lock().await.meta.clone()
  }

  async fn notify_message(&self, msg: Bytes) -> Result<(), Self::Error> {
    self.inner.lock().await.msgs.push(msg);
    Ok(())
  }

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> Result<Vec<Bytes>, Self::Error>
  where
    F: Fn(Bytes) -> (usize, Bytes),
  {
    let mut mu = self.inner.lock().await;
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.broadcasts);
    Ok(out)
  }

  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(self.inner.lock().await.state.clone())
  }

  async fn merge_remote_state(&self, buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    self.inner.lock().await.remote_state = buf;
    Ok(())
  }

  async fn notify_join(&self, _node: Arc<NodeState<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_leave(&self, _node: Arc<NodeState<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_update(&self, _node: Arc<NodeState<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_alive(&self, peer: Arc<NodeState<I, A>>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelAlive => {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(ignore) = &self.ignore {
          if peer.id() == ignore {
            return Ok(());
          }
        }
        tracing::info!(target = "memberlist.mock.delegate", "cancel alive");
        Err(MockDelegateError::CustomAliveCancelled)
      }
      _ => Ok(()),
    }
  }

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<I, A>>,
    other: Arc<NodeState<I, A>>,
  ) -> Result<(), Self::Error> {
    if self.ty == MockDelegateType::NotifyConflict {
      let mut inner = self.inner.lock().await;
      inner.conflict_existing = Some(existing);
      inner.conflict_other = Some(other);
    }

    Ok(())
  }

  async fn notify_merge(&self, _peers: SmallVec<Arc<NodeState<I, A>>>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelMerge => {
        tracing::info!(target = "memberlist.mock.delegate", "cancel merge");
        self.invoked.store(true, Ordering::SeqCst);
        Err(MockDelegateError::CustomMergeCancelled)
      }
      _ => Ok(()),
    }
  }

  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    if self.ty == MockDelegateType::Ping {
      return Ok(Bytes::from_static(b"whatever"));
    }
    Ok(Bytes::new())
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<NodeState<I, A>>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error> {
    if self.ty == MockDelegateType::Ping {
      let mut inner = self.inner.lock().await;
      inner.ping_other = Some(node);
      inner.ping_rtt = rtt;
      inner.ping_payload = payload;
    }
    Ok(())
  }

  #[inline]
  fn disable_promised_pings(&self, _node: &I) -> bool {
    false
  }
}
