use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};

use atomic::Atomic;

#[cfg(feature = "async")]
use async_lock::{Mutex, RwLock};
use bytes::Bytes;
use crossbeam_utils::CachePadded;
#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "async")]
use async_channel::{Receiver, Sender};
#[cfg(not(feature = "async"))]
use crossbeam_channel::{Receiver, Sender};

use crate::{
  awareness::Awareness,
  broadcast::ShowbizBroadcast,
  delegate::Delegate,
  dns::DNS,
  network::META_MAX_SIZE,
  queue::DefaultNodeCalculator,
  timer::Timer,
  transport::Transport,
  types::{Alive, Message, MessageType, Name, Node, NodeId},
  TransmitLimitedQueue,
};

use super::{
  error::Error, state::LocalNodeState, suspicion::Suspicion, types::PushNodeState, Options,
  SecretKeyring,
};

#[cfg(feature = "async")]
mod r#async;
#[cfg(feature = "async")]
pub use r#async::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Status {
  Fresh,
  Running,
  Left,
  Shutdown,
}

#[viewit::viewit]
pub(crate) struct HotData {
  sequence_num: CachePadded<AtomicU32>,
  incarnation: CachePadded<AtomicU32>,
  push_pull_req: CachePadded<AtomicU32>,
  status: CachePadded<Atomic<Status>>,
  num_nodes: Arc<CachePadded<AtomicU32>>,
}

impl HotData {
  fn new() -> Self {
    Self {
      sequence_num: CachePadded::new(AtomicU32::new(0)),
      incarnation: CachePadded::new(AtomicU32::new(0)),
      num_nodes: Arc::new(CachePadded::new(AtomicU32::new(0))),
      push_pull_req: CachePadded::new(AtomicU32::new(0)),
      status: CachePadded::new(Atomic::new(Status::Fresh)),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Advertise {
  addr: SocketAddr,
}

#[viewit::viewit]
pub(crate) struct MessageHandoff {
  msg_ty: MessageType,
  buf: Bytes,
  from: SocketAddr,
}

#[viewit::viewit]
pub(crate) struct MessageQueue {
  /// high priority messages queue
  high: VecDeque<MessageHandoff>,
  /// low priority messages queue
  low: VecDeque<MessageHandoff>,
}

impl MessageQueue {
  const fn new() -> Self {
    Self {
      high: VecDeque::new(),
      low: VecDeque::new(),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Member<S: Spawner> {
  state: LocalNodeState,
  suspicion: Option<Suspicion<S>>,
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct Memberlist<S: Spawner> {
  local: Name,
  /// remote nodes
  nodes: Vec<LocalNodeState>,
  #[allow(clippy::mutable_key_type)]
  node_map: HashMap<Name, Member<S>>,
}

impl<S: Spawner> Memberlist<S> {
  fn new(local: Name) -> Self {
    Self {
      nodes: Vec::new(),
      node_map: HashMap::new(),
      local,
    }
  }

  pub(crate) fn any_alive(&self) -> bool {
    self
      .nodes
      .iter()
      .any(|n| !n.dead_or_left() && n.node.name() != &self.local)
  }
}
