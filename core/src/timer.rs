use futures_channel::oneshot;
use futures_timer::Delay;
use futures_util::{
  future::{BoxFuture, FutureExt},
  select,
};
use std::time::Duration;

pub(crate) struct Timer {
  #[allow(dead_code)]
  fut: oneshot::Receiver<()>,
  cancel: oneshot::Sender<()>,
}

impl Timer {
  pub(crate) fn after<S, F, R>(dur: Duration, future: F, spawner: S) -> Self
  where
    S: Fn(BoxFuture<'static, ()>) -> R,
    F: std::future::Future<Output = ()> + Send + 'static,
  {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (tx, rx) = oneshot::channel();
    (spawner)(
      async move {
        let delay = Delay::new(dur);
        select! {
          _ = delay.fuse() => {
            future.await;
            let _ = tx.send(());
          },
          _ = cancel_rx.fuse() => {},
        }
      }
      .boxed(),
    );

    Timer {
      fut: rx,
      cancel: cancel_tx,
    }
  }

  pub(crate) async fn stop(self) {
    let _ = self.cancel.send(());
  }
}
