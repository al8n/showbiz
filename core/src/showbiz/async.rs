use std::{
  future::Future,
  net::ToSocketAddrs,
  sync::atomic::{AtomicU8, Ordering},
  time::Duration,
};

use crate::{
  dns::{AsyncRuntimeProvider, DnsError},
  transport::TransportError,
  types::Dead,
};

use super::*;

use arc_swap::{strategy::DefaultStrategy, ArcSwapOption, Guard};
use futures_timer::Delay;
use futures_util::{future::BoxFuture, FutureExt};

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct Runtime<T: Transport> {
  transport: T,
  shutdown_tx: Sender<()>,
  advertise: SocketAddr,
}

#[cfg(feature = "async")]
pub(crate) struct AckHandler {
  pub(crate) ack_fn:
    Box<dyn FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  pub(crate) nack_fn: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
  pub(crate) timer: Timer,
}

#[cfg(feature = "async")]
pub trait Spawner: Copy + Unpin + Send + Sync + 'static {
  fn spawn<F>(&self, future: F)
  where
    F::Output: Send + 'static,
    F: Future + Send + 'static;
}

#[derive(Debug, Copy, Clone)]
#[cfg(test)]
pub struct TestSpawner;

#[cfg(test)]
impl Spawner for TestSpawner {
  fn spawn<F>(&self, future: F)
  where
    F::Output: Send + 'static,
    F: Future + Send + 'static,
  {
    tokio::spawn(future);
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<D: Delegate, T: Transport, S: Spawner> {
  id: NodeId,
  hot: HotData,
  awareness: Awareness,
  broadcast: TransmitLimitedQueue<ShowbizBroadcast, DefaultNodeCalculator>,
  // Serializes calls to Leave
  leave_lock: Mutex<()>,
  leave_broadcast_tx: Sender<()>,
  leave_broadcast_rx: Receiver<()>,
  opts: Arc<Options<T>>,
  keyring: Option<SecretKeyring>,
  delegate: Option<D>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue>,
  nodes: Arc<RwLock<Memberlist<S>>>,
  ack_handlers: Arc<Mutex<HashMap<u32, AckHandler>>>,
  dns: Option<DNS<T, S>>,
  metrics_labels: Arc<Vec<metrics::Label>>,
  runtime: ArcSwapOption<Runtime<T>>,
  status: AtomicU8,
  spawner: S,
}

/// Safety: we guarantee that there is no race condition when the transport is modified
unsafe impl<D: Delegate, T: Transport, S: Spawner> Send for ShowbizCore<D, T, S> {}
/// Safety: we guarantee that there is no race condition when the transport is modified
unsafe impl<D: Delegate, T: Transport, S: Spawner> Sync for ShowbizCore<D, T, S> {}

pub struct Showbiz<D: Delegate, T: Transport, S: Spawner> {
  pub(crate) inner: Arc<ShowbizCore<D, T, S>>,
}

impl<D, T, S> Clone for Showbiz<D, T, S>
where
  T: Transport,
  D: Delegate,
  S: Spawner,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  D: Delegate,
  S: Spawner,
{
  #[inline]
  fn ip_must_be_checked(&self) -> bool {
    self
      .inner
      .opts
      .allowed_cidrs
      .as_ref()
      .map(|x| !x.is_empty())
      .unwrap_or(false)
  }
}

impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  S: Spawner,
  D: Delegate,
{
  #[inline]
  pub async fn new(opts: Options<T>, spawner: S) -> Result<Self, Error<D, T>> {
    Self::new_in(None, None, opts, spawner).await
  }

  #[inline]
  pub async fn with_delegate(
    delegate: D,
    opts: Options<T>,
    spawner: S,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), None, opts, spawner).await
  }

  #[inline]
  pub async fn with_keyring(
    keyring: SecretKeyring,
    opts: Options<T>,
    spawner: S,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(None, Some(keyring), opts, spawner).await
  }

  #[inline]
  pub async fn with_delegate_and_keyring(
    delegate: D,
    keyring: SecretKeyring,
    opts: Options<T>,
    spawner: S,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), Some(keyring), opts, spawner).await
  }

  async fn new_in(
    delegate: Option<D>,
    mut keyring: Option<SecretKeyring>,
    opts: Options<T>,
    spawner: S,
  ) -> Result<Self, Error<D, T>> {
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);
    let (leave_broadcast_tx, leave_broadcast_rx) = async_channel::bounded(1);

    if let Some(pk) = opts.secret_key() {
      let has_keyring = keyring.is_some();
      let keyring = keyring.get_or_insert(SecretKeyring::new(vec![], pk));
      if has_keyring {
        let mut mu = keyring.lock().await;
        mu.insert(pk);
        mu.use_key(&pk)?;
      }
    }

    let id = NodeId {
      name: opts.name.clone(),
      port: Some(opts.bind_addr.port()),
      addr: opts.bind_addr.ip().into(),
    };
    let awareness = Awareness::new(
      opts.awareness_max_multiplier as isize,
      Arc::new(vec![]),
      id.clone(),
    );
    let hot = HotData::new();
    let broadcast = TransmitLimitedQueue::new(
      DefaultNodeCalculator::new(hot.num_nodes),
      opts.retransmit_mult,
    );

    let (config, options) = std::fs::read_to_string(opts.dns_config_path.as_path())
      .and_then(|data| trust_dns_resolver::system_conf::parse_resolv_conf(data))
      .map_err(|e| TransportError::Dns(DnsError::from(e)))?;
    let dns = if config.name_servers().is_empty() {
      tracing::warn!(
        target = "showbiz",
        "no DNS servers found in {}",
        opts.dns_config_path.display()
      );

      None
    } else {
      Some(
        DNS::new(config, options, AsyncRuntimeProvider::new(spawner))
          .map_err(Error::dns_resolve)?,
      )
    };

    Ok(Showbiz {
      inner: Arc::new(ShowbizCore {
        id,
        awareness,
        broadcast,
        hot: HotData::new(),
        dns,
        leave_lock: Mutex::new(()),
        delegate,
        keyring,
        handoff_tx,
        handoff_rx,
        leave_broadcast_tx,
        leave_broadcast_rx,
        queue: Mutex::new(MessageQueue::new()),
        nodes: Arc::new(RwLock::new(Memberlist::new(opts.name.clone()))),
        opts: Arc::new(opts),
        ack_handlers: Arc::new(Mutex::new(HashMap::new())),
        spawner,
        metrics_labels: Arc::new(vec![]),
        status: AtomicU8::new(0),
        runtime: ArcSwapOption::empty(),
      }),
    })
  }

  #[inline]
  pub async fn bootstrap(&self) -> Result<(), Error<D, T>> {
    // if we already in running status, just return
    if self.inner.status.load(Ordering::SeqCst) == 1 {
      return Ok(());
    }

    let transport = T::new(self.inner.opts.transport.clone()).await?;

    // Get the final advertise address from the transport, which may need
    // to see which address we bound to. We'll refresh this each time we
    // send out an alive message.
    let advertise = transport.final_advertise_addr(self.inner.opts.advertise_addr)?;
    let encryption_enabled = self.encryption_enabled().await;

    if advertise.ip().is_global() && !encryption_enabled {
      tracing::warn!(
        target = "showbiz",
        "binding to public address without encryption!"
      );
    }

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let runtime = Runtime {
      transport,
      shutdown_tx,
      advertise,
    };
    self.inner.runtime.store(Some(Arc::new(runtime)));

    self.stream_listener(shutdown_rx.clone());
    self.packet_handler(shutdown_rx.clone());
    self.packet_listener(shutdown_rx.clone());

    let meta = if let Some(d) = &self.inner.delegate {
      d.node_meta(META_MAX_SIZE)
    } else {
      Bytes::new()
    };

    if meta.len() > META_MAX_SIZE {
      self.inner.runtime.store(None);
      panic!("Node meta data provided is longer than the limit");
    }

    let alive = Alive {
      incarnation: self.next_incarnation(),
      vsn: self.inner.opts.build_vsn_array(),
      meta,
      node: self.inner.id.clone(),
    };

    self.alive_node(alive, None, true).await;
    self.schedule(shutdown_rx).await;
    Ok(())
  }

  /// Returns a list of all known live nodes.
  #[inline]
  pub async fn members(&self) -> Vec<Arc<Node>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .map(|n| n.node.clone())
      .collect()
  }

  /// Returns the number of alive nodes currently known. Between
  /// the time of calling this and calling Members, the number of alive nodes
  /// may have changed, so this shouldn't be used to determine how many
  /// members will be returned by Members.
  #[inline]
  pub async fn num_members(&self) -> usize {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| !n.dead_or_left())
      .count()
  }

  /// Leave will broadcast a leave message but will not shutdown the background
  /// listeners, meaning the node will continue participating in gossip and state
  /// updates.
  ///
  /// This will block until the leave message is successfully broadcasted to
  /// a member of the cluster, if any exist or until a specified timeout
  /// is reached.
  ///
  /// This method is safe to call multiple times, but must not be called
  /// after the cluster is already shut down.
  pub async fn leave(&self, timeout: Duration) -> Result<(), Error<D, T>> {
    let _mu = self.inner.leave_lock.lock().await;

    if !self.has_left() {
      self.inner.hot.leave.fetch_add(1, Ordering::SeqCst);

      let mut memberlist = self.inner.nodes.write().await;
      if let Some(state) = memberlist.node_map.get(&memberlist.local) {
        // This dead message is special, because Node and From are the
        // same. This helps other nodes figure out that a node left
        // intentionally. When Node equals From, other nodes know for
        // sure this node is gone.

        let d = Dead {
          incarnation: state.state.incarnation.load(Ordering::Relaxed),
          node: state.state.node.id.clone(),
          from: state.state.node.id.clone(),
        };

        self.dead_node(&mut memberlist, d).await?;

        // Block until the broadcast goes out
        if memberlist.any_alive() {
          if timeout > Duration::ZERO {
            futures_util::select_biased! {
              rst = self.inner.leave_broadcast_rx.recv().fuse() => {
                if let Err(e) = rst {
                  tracing::error!(
                    target = "showbiz",
                    "failed to receive leave broadcast: {}",
                    e
                  );
                }
              },
              _ = futures_timer::Delay::new(timeout).fuse() => {
                return Err(Error::LeaveTimeout);
              }
            }
          } else if let Err(e) = self.inner.leave_broadcast_rx.recv().await {
            tracing::error!(
              target = "showbiz",
              "failed to receive leave broadcast: {}",
              e
            );
          }
        }
      } else {
        tracing::warn!(target = "showbiz", "leave but we're not a member");
      }
    }
    Ok(())
  }

  /// Used to take an existing Memberlist and attempt to join a cluster
  /// by contacting all the given hosts and performing a state sync. Initially,
  /// the Memberlist only contains our own state, so doing this will cause
  /// remote nodes to become aware of the existence of this node, effectively
  /// joining the cluster.
  ///
  /// This returns the number of hosts successfully contacted and an error if
  /// none could be reached. If an error is returned, the node did not successfully
  /// join the cluster.
  pub async fn join(&self, existing: Vec<NodeId>) -> Result<usize, Vec<Error<D, T>>> {
    let mut num_success = 0;
    let mut errors = Vec::new();
    for exist in existing {
      let addrs = match self.resolve_addr(exist.clone()).await {
        Ok(addrs) => addrs,
        Err(e) => {
          tracing::debug!(
            target = "showbiz",
            err = %e,
            "failed to resolve address {}",
            exist
          );
          errors.push(e);
          continue;
        }
      };

      for (name, addr) in addrs {
        let id = NodeId {
          name,
          port: Some(addr.port()),
          addr: addr.ip().into(),
        };
        if let Err(e) = self.push_pull_node(&id, true).await {
          tracing::debug!(
            target = "showbiz",
            err = %e,
            "failed to join {}({})",
            id.name.as_ref(),
            addr
          );
          errors.push(e);
        } else {
          num_success += 1;
        }
      }
    }

    if num_success == 0 {
      return Err(errors);
    }

    Ok(num_success)
  }

  /// Gives this instance's idea of how well it is meeting the soft
  /// real-time requirements of the protocol. Lower numbers are better, and zero
  /// means "totally healthy".
  #[inline]
  pub async fn health_score(&self) -> usize {
    self.inner.awareness.get_health_score().await as usize
  }

  /// Used to trigger re-advertising the local node. This is
  /// primarily used with a Delegate to support dynamic updates to the local
  /// meta data.  This will block until the update message is successfully
  /// broadcasted to a member of the cluster, if any exist or until a specified
  /// timeout is reached.
  pub async fn update_node(&self, timeout: Duration) -> Result<(), Error<D, T>> {
    // Get the node meta data
    let meta = if let Some(delegate) = &self.inner.delegate {
      let meta = delegate.node_meta(META_MAX_SIZE);
      if meta.len() > META_MAX_SIZE {
        panic!("node meta data provided is longer than the limit");
      }
      meta
    } else {
      Bytes::new()
    };

    // Get the existing node
    // unwrap safe here this is self
    let node_id = self
      .inner
      .nodes
      .read()
      .await
      .node_map
      .get(&self.inner.id.name)
      .unwrap()
      .state
      .id()
      .clone();

    // Format a new alive message
    let alive = Alive {
      incarnation: self.next_incarnation(),
      node: node_id,
      meta,
      vsn: self.inner.opts.build_vsn_array(),
    };
    let (notify_tx, notify_rx) = async_channel::bounded(1);
    self.alive_node(alive, Some(notify_tx), true).await;

    // Wait for the broadcast or a timeout
    if self.any_alive().await {
      if timeout > Duration::ZERO {
        futures_util::select_biased! {
          _ = notify_rx.recv().fuse() => {},
          _ = Delay::new(timeout).fuse() => return Err(Error::UpdateTimeout),
        }
      } else {
        futures_util::select! {
          _ = notify_rx.recv().fuse() => {},
        }
      }
    }

    Ok(())
  }

  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  #[inline]
  pub async fn send(&self, to: NodeId, msg: Message) -> Result<(), Error<D, T>> {
    self.raw_send_msg_packet(&to, msg.0).await
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_reliable(&self, to: &Node, msg: Message) -> Result<(), Error<D, T>> {
    self.send_user_msg(to.id(), msg).await
  }

  pub async fn shutdown(&self) -> Result<(), Error<D, T>> {
    // // Shut down the transport first, which should block until it's
    // // completely torn down. If we kill the memberlist-side handlers
    // // those I/O handlers might get stuck.
    // let Self { inner: core } = self;

    // while Arc::strong_count(&core) > 1 {
    //   parker.await;
    // }

    // let ShowbizCore {
    //   hot,

    //   shutdown_tx,

    //   transport,
    //   ..
    // } = Arc::into_inner(core).unwrap();

    // // Shut down the transport first, which should block until it's
    // // completely torn down. If we kill the memberlist-side handlers
    // // those I/O handlers might get stuck.
    // transport.shutdown().await.map_err(Error::transport)?;

    // // Now tear down everything else.
    // hot.shutdown.store(1, Ordering::SeqCst);
    // drop(shutdown_tx);

    Ok(())
  }
}

// private impelementation
impl<D, T, S> Showbiz<D, T, S>
where
  D: Delegate,
  T: Transport,
  S: Spawner,
{
  #[inline]
  pub(crate) fn runtime(&self) -> Guard<Option<Arc<Runtime<T>>>, DefaultStrategy> {
    self.inner.runtime.load()
  }

  /// a helper to initiate a TCP-based DNS lookup for the given host.
  /// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
  /// the response has the truncate bit set, which isn't common on DNS servers like
  /// Consul's. By doing the TCP lookup directly, we get the best chance for the
  /// largest list of hosts to join. Since joins are relatively rare events, it's ok
  /// to do this rather expensive operation.
  pub(crate) async fn tcp_lookup_ip(
    &self,
    dns: &DNS<T, S>,
    host: &str,
    default_port: u16,
    node_name: &Name,
  ) -> Result<Vec<(Name, SocketAddr)>, Error<D, T>> {
    // Don't attempt any TCP lookups against non-fully qualified domain
    // names, since those will likely come from the resolv.conf file.
    if !host.contains('.') {
      return Ok(Vec::new());
    }

    // Make sure the domain name is terminated with a dot (we know there's
    // at least one character at this point).
    let dn = host.chars().last().unwrap();
    let ips = if dn != '.' {
      let mut dn = host.to_string();
      dn.push('.');
      dns.lookup_ip(dn).await
    } else {
      dns.lookup_ip(host).await
    }
    .map_err(Error::dns_resolve)?;

    Ok(
      ips
        .into_iter()
        .map(|ip| {
          let addr = SocketAddr::new(ip, default_port);
          (node_name.clone(), addr)
        })
        .collect(),
    )
  }

  /// Used to resolve the address into an address,
  /// port, and error. If no port is given, use the default
  pub(crate) async fn resolve_addr(
    &self,
    mut host: NodeId,
  ) -> Result<Vec<(Name, SocketAddr)>, Error<D, T>> {
    // This captures the supplied port, or the default one.
    if host.port().is_none() {
      host = host.set_port(Some(self.inner.opts.bind_addr.port()));
    }

    let NodeId { name, port, addr } = host;

    // If it looks like an IP address we are done.
    if addr.is_ip() {
      return Ok(vec![(
        name.clone(),
        SocketAddr::new(addr.unwrap_ip(), port.unwrap()),
      )]);
    }

    // First try TCP so we have the best chance for the largest list of
    // hosts to join. If this fails it's not fatal since this isn't a standard
    // way to query DNS, and we have a fallback below.
    if let Some(dns) = self.inner.dns.as_ref() {
      match self
        .tcp_lookup_ip(dns, addr.unwrap_domain(), port.unwrap(), &name)
        .await
      {
        Ok(ips) => {
          if !ips.is_empty() {
            return Ok(ips);
          }
        }
        Err(e) => {
          tracing::debug!(
            target = "showbiz",
            "TCP-first lookup failed for '{}', falling back to UDP: {}",
            addr,
            e
          );
        }
      }
    }

    // If TCP didn't yield anything then use the normal Go resolver which
    // will try UDP, then might possibly try TCP again if the UDP response
    // indicates it was truncated.
    addr
      .unwrap_domain()
      .to_socket_addrs()
      .map_err(|e| Error::Transport(TransportError::Dns(DnsError::IO(e))))
      .map(|addrs| addrs.into_iter().map(|addr| (name.clone(), addr)).collect())
  }

  #[inline]
  pub(crate) async fn get_advertise(&self) -> SocketAddr {
    // Unwrap is safe here, because advertise is always set before get_advertise is called.
    self.inner.runtime.load().as_ref().unwrap().advertise
  }

  /// Check for any other alive node.
  #[inline]
  pub(crate) async fn any_alive(&self) -> bool {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .any(|n| !n.dead_or_left() && n.node.name() != self.inner.opts.name.as_ref())
  }

  pub(crate) async fn encryption_enabled(&self) -> bool {
    if let Some(keyring) = &self.inner.keyring {
      !keyring.lock().await.is_empty() && !self.inner.opts.encryption_algo.is_none()
    } else {
      false
    }
  }

  pub(crate) async fn verify_protocol(&self, _remote: &[PushNodeState]) -> Result<(), Error<D, T>> {
    // TODO: implement

    Ok(())
  }

  #[cfg(test)]
  pub(crate) async fn change_node<F>(&self, _addr: SocketAddr, _f: F)
  where
    F: Fn(&LocalNodeState),
  {
    // let mut nodes = self.inner.nodes.write().await;
    // if let Some(n) = nodes.node_map.get_mut(&addr) {
    //   f(n)
    // }
  }
}
