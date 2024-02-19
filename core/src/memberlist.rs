use std::{
  collections::{HashMap, VecDeque},
  sync::{
    atomic::{AtomicBool, AtomicU32},
    Arc,
  },
  time::Instant,
};

use agnostic::Runtime;
use async_channel::{Receiver, Sender};
use bytes::Bytes;
use nodecraft::Node;

use crate::types::TinyVec;

use super::{
  awareness::Awareness,
  broadcast::MemberlistBroadcast,
  delegate::Delegate,
  error::Error,
  network::META_MAX_SIZE,
  queue::TransmitLimitedQueue,
  // security::SecretKeyring,
  state::LocalServerState,
  suspicion::Suspicion,
  timer::Timer,
  transport::Transport,
  types::{Alive, Message, Server},
  Options,
};

mod r#async;
pub use r#async::*;

#[viewit::viewit]
pub(crate) struct HotData {
  sequence_num: AtomicU32,
  incarnation: AtomicU32,
  push_pull_req: AtomicU32,
  shutdown: AtomicBool,
  leave: AtomicBool,
  num_nodes: Arc<AtomicU32>,
}

impl HotData {
  fn new() -> Self {
    Self {
      sequence_num: AtomicU32::new(0),
      incarnation: AtomicU32::new(0),
      num_nodes: Arc::new(AtomicU32::new(0)),
      push_pull_req: AtomicU32::new(0),
      shutdown: AtomicBool::new(false),
      leave: AtomicBool::new(false),
    }
  }
}

#[viewit::viewit]
pub(crate) struct MessageHandoff<I, A> {
  msg: Message<I, A>,
  from: A,
}

#[viewit::viewit]
pub(crate) struct MessageQueue<I, A> {
  /// high priority messages queue
  high: VecDeque<MessageHandoff<I, A>>,
  /// low priority messages queue
  low: VecDeque<MessageHandoff<I, A>>,
}

impl<I, A> MessageQueue<I, A> {
  const fn new() -> Self {
    Self {
      high: VecDeque::new(),
      low: VecDeque::new(),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Member<I, A, R> {
  pub(crate) state: LocalServerState<I, A>,
  pub(crate) suspicion: Option<Suspicion<I, R>>,
}

impl<I, A, R> core::ops::Deref for Member<I, A, R> {
  type Target = LocalServerState<I, A>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

pub(crate) struct Members<I, A, R> {
  pub(crate) local: Node<I, A>,
  pub(crate) nodes: TinyVec<Member<I, A, R>>,
  pub(crate) node_map: HashMap<I, usize>,
}

impl<I, A, Run: Runtime> rand::seq::SliceRandom for Members<I, A, Run>
where
  I: Eq + core::hash::Hash,
{
  type Item = Member<I, A, Run>;

  fn choose<R>(&self, _rng: &mut R) -> Option<&Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_mut<R>(&mut self, _rng: &mut R) -> Option<&mut Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_multiple<R>(
    &self,
    _rng: &mut R,
    _amount: usize,
  ) -> rand::seq::SliceChooseIter<Self, Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_weighted<R, F, B, X>(
    &self,
    _rng: &mut R,
    _weight: F,
  ) -> Result<&Self::Item, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> B,
    B: rand::distributions::uniform::SampleBorrow<X>,
    X: rand::distributions::uniform::SampleUniform
      + for<'a> core::ops::AddAssign<&'a X>
      + core::cmp::PartialOrd<X>
      + Clone
      + Default,
  {
    unreachable!()
  }

  fn choose_weighted_mut<R, F, B, X>(
    &mut self,
    _rng: &mut R,
    _weight: F,
  ) -> Result<&mut Self::Item, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> B,
    B: rand::distributions::uniform::SampleBorrow<X>,
    X: rand::distributions::uniform::SampleUniform
      + for<'a> core::ops::AddAssign<&'a X>
      + core::cmp::PartialOrd<X>
      + Clone
      + Default,
  {
    unreachable!()
  }

  fn choose_multiple_weighted<R, F, X>(
    &self,
    _rng: &mut R,
    _amount: usize,
    _weight: F,
  ) -> Result<rand::seq::SliceChooseIter<Self, Self::Item>, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> X,
    X: Into<f64>,
  {
    unreachable!()
  }

  fn shuffle<R>(&mut self, rng: &mut R)
  where
    R: rand::Rng + ?Sized,
  {
    // Sample a number uniformly between 0 and `ubound`. Uses 32-bit sampling where
    // possible, primarily in order to produce the same output on 32-bit and 64-bit
    // platforms.
    #[inline]
    fn gen_index<R: rand::Rng + ?Sized>(rng: &mut R, ubound: usize) -> usize {
      if ubound <= (core::u32::MAX as usize) {
        rng.gen_range(0..ubound as u32) as usize
      } else {
        rng.gen_range(0..ubound)
      }
    }

    for i in (1..self.nodes.len()).rev() {
      // invariant: elements with index > i have been locked in place.
      let ridx = gen_index(rng, i + 1);
      let curr = self.node_map.get_mut(self.nodes[i].state.id()).unwrap();
      *curr = ridx;
      let target = self.node_map.get_mut(self.nodes[ridx].state.id()).unwrap();
      *target = i;
      self.nodes.swap(i, ridx);
    }
  }

  fn partial_shuffle<R>(
    &mut self,
    _rng: &mut R,
    _amount: usize,
  ) -> (&mut [Self::Item], &mut [Self::Item])
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }
}

impl<I, A, R> Members<I, A, R> {
  fn new(local: Node<I, A>) -> Self {
    Self {
      nodes: TinyVec::new(),
      node_map: HashMap::new(),
      local,
    }
  }
}

impl<I: PartialEq, A, R> Members<I, A, R> {
  pub(crate) fn any_alive(&self) -> bool {
    self
      .nodes
      .iter()
      .any(|m| !m.dead_or_left() && m.id().ne(self.local.id()))
  }
}

#[cfg(any(test, feature = "test"))]
impl<I: Eq + core::hash::Hash, A, R> Members<I, A, R> {
  pub(crate) fn get_state<Q>(&self, id: &Q) -> Option<LocalServerState<I, A>>
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    self
      .node_map
      .get(id)
      .map(|idx| self.nodes[*idx].state.clone())
  }

  pub(crate) fn set_state<Q>(&mut self, id: &Q, new_state: crate::types::ServerState)
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    if let Some(idx) = self.node_map.get(id) {
      let state = &mut self.nodes[*idx].state;
      state.state = new_state;
    }
  }
}
