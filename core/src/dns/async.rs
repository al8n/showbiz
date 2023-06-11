use crate::{showbiz::Spawner, transport::Transport};
use futures_timer::Delay;
use futures_util::{select_biased, FutureExt};
use std::{future::Future, io, marker::PhantomData, net::SocketAddr, time::Duration};
use trust_dns_proto::Time;
use trust_dns_resolver::{
  name_server::{RuntimeProvider, Spawn},
  AsyncResolver,
};

pub(crate) type Dns<T, S> = AsyncResolver<AsyncRuntimeProvider<T, S>>;

#[derive(Debug, thiserror::Error)]
pub enum DnsError {
  #[error("{0}")]
  IO(#[from] std::io::Error),
  #[error("{0}")]
  Resolve(#[from] trust_dns_resolver::error::ResolveError),
}

#[repr(transparent)]
pub(crate) struct AsyncSpawn<S: Spawner> {
  spawner: S,
}

impl<S: Spawner> Clone for AsyncSpawn<S> {
  fn clone(&self) -> Self {
    Self {
      spawner: self.spawner,
    }
  }
}

impl<S: Spawner> Copy for AsyncSpawn<S> {}

impl<S: Spawner> Spawn for AsyncSpawn<S> {
  fn spawn_bg<F>(&mut self, future: F)
  where
    F: Future<Output = Result<(), trust_dns_proto::error::ProtoError>> + Send + 'static,
  {
    self.spawner.spawn(async move {
      if let Err(e) = future.await {
        tracing::error!(target = "showbiz", err = %e, "dns error");
      }
    });
  }
}

pub(crate) struct AsyncRuntimeProvider<T: Transport, S: Spawner> {
  spawner: AsyncSpawn<S>,
  _marker: PhantomData<T>,
}

impl<T: Transport, S: Spawner> AsyncRuntimeProvider<T, S> {
  pub(crate) fn new(spawner: S) -> Self {
    Self {
      spawner: AsyncSpawn { spawner },
      _marker: PhantomData,
    }
  }
}

impl<T, S> Clone for AsyncRuntimeProvider<T, S>
where
  T: Transport,
  S: Spawner,
{
  fn clone(&self) -> Self {
    Self {
      spawner: self.spawner,
      _marker: PhantomData,
    }
  }
}

pub(crate) struct Timer;

#[async_trait::async_trait]
impl Time for Timer {
  /// Return a type that implements `Future` that will wait until the specified duration has
  /// elapsed.
  async fn delay_for(duration: Duration) {
    Delay::new(duration).await
  }

  /// Return a type that implement `Future` to complete before the specified duration has elapsed.
  async fn timeout<F: 'static + Future + Send>(
    duration: Duration,
    future: F,
  ) -> Result<F::Output, std::io::Error> {
    select_biased! {
      rst = future.fuse() => {
        return Ok(rst);
      }
      _ = Delay::new(duration).fuse() => {
        return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"));
      }
    }
  }
}

impl<T: Transport + Unpin, S: Spawner> RuntimeProvider for AsyncRuntimeProvider<T, S> {
  type Handle = AsyncSpawn<S>;

  type Timer = Timer;

  type Udp = T::UnreliableConnection;

  type Tcp = T::Connection;

  fn create_handle(&self) -> Self::Handle {
    self.spawner
  }

  fn connect_tcp(
    &self,
    addr: SocketAddr,
  ) -> std::pin::Pin<Box<dyn Send + Future<Output = io::Result<Self::Tcp>>>> {
    Box::pin(<T as Transport>::connect(addr))
  }

  fn bind_udp(
    &self,
    local_addr: SocketAddr,
    _server_addr: SocketAddr,
  ) -> std::pin::Pin<Box<dyn Send + Future<Output = io::Result<Self::Udp>>>> {
    Box::pin(<T as Transport>::bind_unreliable(local_addr))
  }
}
