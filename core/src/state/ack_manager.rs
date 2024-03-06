use std::{
  collections::HashMap,
  sync::Arc,
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;

use crate::{
  timer::Timer,
  types::{Ack, Nack},
};

#[viewit::viewit]
pub(crate) struct AckMessage {
  complete: bool,
  payload: Bytes,
  timestamp: Instant,
}

pub(crate) struct AckHandler {
  pub(crate) ack_fn:
    Box<dyn FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  pub(crate) nack_fn: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
  pub(crate) timer: Timer,
}

#[derive(Clone)]
pub(crate) struct AckManager(pub(super) Arc<Mutex<HashMap<u32, AckHandler>>>);

impl AckManager {
  pub(crate) fn new() -> Self {
    Self(Arc::new(Mutex::new(HashMap::new())))
  }

  #[inline]
  pub(crate) async fn invoke_ack_handler(&self, ack: Ack, timestamp: Instant) {
    let (seq_no, payload) = ack.into_components();
    let ah = self.0.lock().remove(&seq_no);
    if let Some(handler) = ah {
      handler.timer.stop().await;
      (handler.ack_fn)(payload, timestamp).await;
    }
  }

  #[inline]
  pub(crate) async fn invoke_nack_handler(&self, nack: Nack) {
    let ah = self
      .0
      .lock()
      .get(&nack.sequence_number())
      .and_then(|ah| ah.nack_fn.clone());
    if let Some(nack_fn) = ah {
      (nack_fn)().await;
    }
  }

  #[inline]
  pub(crate) fn set_ack_handler<F, R: Runtime>(&self, sequence_number: u32, timeout: Duration, f: F)
  where
    F: FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static,
  {
    // Add the handler
    let tlock = self.clone();
    let mut mu = self.0.lock();
    mu.insert(
      sequence_number,
      AckHandler {
        ack_fn: Box::new(f),
        nack_fn: None,
        timer: Timer::after::<_, R>(timeout, async move {
          tlock.0.lock().remove(&sequence_number);
        }),
      },
    );
  }

  /// Used to attach the ackCh to receive a message when an ack
  /// with a given sequence number is received. The `complete` field of the message
  /// will be false on timeout. Any nack messages will cause an empty struct to be
  /// passed to the nackCh, which can be nil if not needed.
  pub(crate) fn set_probe_channels<R>(
    &self,
    sequence_number: u32,
    ack_tx: async_channel::Sender<AckMessage>,
    nack_tx: Option<async_channel::Sender<()>>,
    sent: Instant,
    timeout: Duration,
  ) where
    R: Runtime,
  {
    let tx = ack_tx.clone();
    let ack_fn = |payload, timestamp| {
      async move {
        futures::select! {
          _ = tx.send(AckMessage {
            payload,
            timestamp,
            complete: true,
          }).fuse() => {},
          default => {}
        }
      }
      .boxed()
    };

    let nack_fn = move || {
      let tx = nack_tx.clone();
      async move {
        if let Some(nack_tx) = tx {
          futures::select! {
            _ = nack_tx.send(()).fuse() => {},
            default => {}
          }
        }
      }
      .boxed()
    };

    let ack_manager = self.clone();
    self.insert(
      sequence_number,
      AckHandler {
        ack_fn: Box::new(ack_fn),
        nack_fn: Some(Arc::new(nack_fn)),
        timer: Timer::after::<_, R>(timeout, async move {
          ack_manager.remove(sequence_number);
          futures::select! {
            _ = ack_tx.send(AckMessage {
              payload: Bytes::new(),
              timestamp: sent,
              complete: false,
            }).fuse() => {},
            default => {}
          }
        }),
      },
    );
  }

  #[inline]
  pub(crate) fn insert(&self, sequence_number: u32, handler: AckHandler) {
    self.0.lock().insert(sequence_number, handler);
  }

  #[inline]
  pub(crate) fn remove(&self, sequence_number: u32) {
    self.0.lock().remove(&sequence_number);
  }
}
