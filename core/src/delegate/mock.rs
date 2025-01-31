use futures::lock::Mutex;

use super::*;

struct MockDelegateInner<W> {
  meta: Meta,
  msgs: Vec<Bytes>,
  broadcasts: TinyVec<Bytes>,
  state: Bytes,
  remote_state: Bytes,
  _marker: std::marker::PhantomData<W>,
}

impl<W> Default for MockDelegateInner<W> {
  fn default() -> Self {
    Self {
      meta: Meta::empty(),
      msgs: vec![],
      broadcasts: TinyVec::new(),
      state: Bytes::new(),
      remote_state: Bytes::new(),
      _marker: std::marker::PhantomData,
    }
  }
}

#[doc(hidden)]
pub struct MockDelegate<W> {
  inner: Arc<Mutex<MockDelegateInner<W>>>,
}

impl<W> Default for MockDelegate<W> {
  fn default() -> Self {
    Self::new()
  }
}

impl<W> MockDelegate<W> {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
    }
  }

  pub fn with_meta(meta: Meta) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        meta,
        ..Default::default()
      })),
    }
  }

  pub fn with_state(state: Bytes) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        state,
        ..Default::default()
      })),
    }
  }

  pub fn with_state_and_broadcasts(state: Bytes, broadcasts: TinyVec<Bytes>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        state,
        broadcasts,
        ..Default::default()
      })),
    }
  }
}

impl<W> MockDelegate<W> {
  pub async fn set_meta(&self, meta: Meta) {
    self.inner.lock().await.meta = meta;
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
}

impl<W> NodeDelegate for MockDelegate<W>
where
  W::Id: Send + Sync + 'static,
  W::Address: CheapClone + Send + Sync + 'static,
  W: Wire + Send + Sync + 'static,
{
  type Wire = W;

  async fn node_meta(&self, _limit: usize) -> Meta {
    self.inner.lock().await.meta.clone()
  }

  async fn notify_message(&self, msg: Bytes) {
    self.inner.lock().await.msgs.push(msg);
  }

  async fn broadcast_messages<F>(
    &self,
    _: usize,
    _: usize,
    mut encoded_len: F,
  ) -> Result<(), <Self::Wire as Wire>::Error>
  where
    F: FnMut(Bytes) -> Result<usize, <Self::Wire as Wire>::Error> + Send,
  {
    let mut mu = self.inner.lock().await;
    let mut out = TinyVec::new();
    core::mem::swap(&mut out, &mut mu.broadcasts);
    for msg in out {
      encoded_len(msg)?;
    }

    Ok(())
  }

  async fn local_state(&self, _join: bool) -> Bytes {
    self.inner.lock().await.state.clone()
  }

  async fn merge_remote_state(&self, buf: Bytes, _join: bool) {
    self.inner.lock().await.remote_state = buf;
  }
}
