use std::{
  net::SocketAddr,
  ops::Sub,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{Future, FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone, Node};

use crate::{
  broadcast::Broadcast,
  delegate::VoidDelegate,
  error::Error,
  state::{AckManager, LocalNodeState},
  tests::get_memberlist,
  transport::Transport,
  types::{Ack, Alive, Dead, Message, Nack, State, Suspect},
  Memberlist, Options,
};

async fn host_memberlist<T, R: Runtime>(
  t: T,
  opts: Options,
) -> Result<
  Memberlist<T>,
  Error<T, VoidDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
>
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  Memberlist::new_in(t, None, opts).await.map(|(_, _, t)| t)
}

/// Unit test to test the probe functionality
pub async fn probe<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // should ping addr2
  m1.probe().await;

  // Should not be marked suspect
  let nodes = m1.inner.nodes.read().await;
  let idx = *nodes.node_map.get(&m2.inner.id).unwrap();
  let n = &nodes.nodes[idx];
  assert_eq!(n.state.state, State::Alive);

  // Should increment seqno
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node suspect functionality
pub async fn probe_node_suspect<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  suspect_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let m3: Memberlist<T> = host_memberlist(t3, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m3.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a3, None, false).await;

  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: suspect_node.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m1.alive_node(a4, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(suspect_node.id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(suspect_node.id())
    .unwrap()
    .state;
  // Should be marked suspect.
  assert_eq!(state, State::Suspect, "bad state: {state}");
  R::sleep(Duration::from_millis(1000)).await;

  // One of the peers should have attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(
    s2 == 1 && s3 == 1,
    "bad seqnos, expected both to be 1: {s2}, {s3}"
  );

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

pub async fn test_probe_node_dogpile() {
  todo!()
}

/// Unit test to test the probe node awareness degraded functionality
pub async fn probe_node_awareness_degraded<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let probe_time_min = Duration::from_millis(200) * 2 - Duration::from_millis(50);

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m3: Memberlist<T> = host_memberlist(
    t3,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m3.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a3, None, false).await;

  // Node 4 never gets started.
  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node4.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m1.alive_node(a4, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1).await;
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m4.
  let start_probe = {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    let start = Instant::now();
    m1.probe_node(&n).await;
    start
  };

  let probe_time = start_probe.elapsed();

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, State::Suspect, "expect node to be suspect");
  }

  // Make sure we timed out approximately on time (note that we accounted
  // for the slowed-down failure detector in the probeTimeMin calculation.
  assert!(
    probe_time >= probe_time_min,
    "probed too quickly: {}s",
    probe_time.as_secs_f64()
  );

  // Confirm at least one of the peers attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(s2 == 1 && s3 == 1, "bad seqnos: {s2}, {s3}");

  // We should have gotten all the nacks, so our score should remain the
  // same, since we didn't get a successful probe.
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness improved functionality
pub async fn probe_node_awareness_improved<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1).await;
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m2.
  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Node should be reported alive.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap()
      .state;
    assert_eq!(state, State::Alive, "expect node to be alive");
  }

  // Our score should have improved since we did a good probe.
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 0, "bad: {score}");

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness missed nack functionality
pub async fn probe_node_awareness_missed_nack<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t2_opts: Options,
  node3: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let probe_time_max = Duration::from_millis(200) + Duration::from_millis(50);

  let m2: Memberlist<T> = host_memberlist(
    t2,
    t2_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // Node 3 and node 4 never get started.
  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node3,
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node4.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  // Node 3 and node 4 never get started.
  m1.alive_node(a3, None, false).await;
  m1.alive_node(a4, None, false).await;

  // Make sure health looks good
  let health = m1.inner.awareness.get_health_score().await;
  assert_eq!(health, 0, "bad: {health}");

  // Have node m1 probe m4.
  let start_probe = {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    let start = Instant::now();
    m1.probe_node(&n).await;
    start
  };
  let probe_time = start_probe.elapsed();

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, State::Suspect, "expect node to be suspect");
  }

  // Make sure we timed out approximately on time.
  assert!(
    probe_time <= probe_time_max,
    "took to long to probe: {}",
    probe_time.as_secs_f64()
  );

  for i in 0..10 {
    let score = m1.inner.awareness.get_health_score().await;
    if score == 1 {
      break;
    }
    if i == 9 {
      panic!("expected health score to decrement on missed nack. want 1  got {score}");
    }
    R::sleep(Duration::from_millis(25)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

pub async fn probe_node_buddy<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2.cheap_clone(), None, false).await;
  m2.alive_node(a2, None, true).await;

  // Force the state to suspect so we piggyback a suspect message with the ping.
  // We should see this get refuted later, and the ping will succeed.
  {
    let mut members = m1.inner.nodes.write().await;
    let id = m2.local_id();
    members.set_state(id, State::Suspect);
    let n = members.get_state(id).unwrap();
    drop(members);
    m1.probe_node(&n).await;
  };

  // Make sure a ping was sent.
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");

  // Check a broadcast is queued.
  let num = m2.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Should be alive msg
  let broadcasts = m2.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe functionality
pub async fn probe_node<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Should not be marked alive
  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(m2.local_id())
    .unwrap()
    .state;
  assert_eq!(state, State::Alive, "expect node to be alive: {state}");

  // Should increment seqno
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");
}

/// Unit test to test the ping functionality
pub async fn ping<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  bad_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_secs(1))
      .with_probe_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // Do a legit ping.
  let rtt = {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.ping(n.node()).await.unwrap()
  };

  assert!(rtt > Duration::ZERO, "bad: {rtt:?}");

  // This ping has a bad node name so should timeout.
  let err = m1.ping(bad_node.cheap_clone()).await.unwrap_err();
  assert!(matches!(err, Error::Lost(_)), "bad: {err}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the reset nodes functionality
pub async fn reset_nodes<T, R>(
  t1: T,
  t1_opts: Options,
  n1: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  n2: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  n3: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts.with_gossip_to_the_dead_time(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: n1,
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, false).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: n2.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: n3,
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a3, None, false).await;

  let d = Dead {
    incarnation: 1,
    node: n2.id().cheap_clone(),
    from: m1.local_id().cheap_clone(),
  };
  {
    let mut members = m1.inner.nodes.write().await;
    m1.dead_node(&mut *members, d).await.unwrap();
  }

  m1.reset_nodes().await;

  {
    let nodes = m1.inner.nodes.read().await;
    assert_eq!(nodes.node_map.len(), 3, "bad: {}", nodes.node_map.len());
    assert!(
      nodes.node_map.get(n2.id()).is_some(),
      "{} should not be unmapped",
      n2
    );
  }

  R::sleep(Duration::from_millis(200)).await;
  m1.reset_nodes().await;
  {
    let nodes = m1.inner.nodes.read().await;
    assert_eq!(nodes.node_map.len(), 2, "bad: {}", nodes.node_map.len());
    assert!(
      nodes.node_map.get(n2.id()).is_none(),
      "{} should be unmapped",
      n2
    );
  }

  m1.shutdown().await.unwrap();
}

async fn ack_handler_exists(m: &AckManager, idx: u32) -> bool {
  let acks = m.0.lock();
  acks.contains_key(&idx)
}

/// Unit test to test the set probe channels functionality
pub async fn set_probe_channels<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m = AckManager::new();

  let (tx, _rx) = async_channel::bounded(1);

  m.set_probe_channels::<R>(0, tx, None, Instant::now(), Duration::from_millis(10));

  assert!(ack_handler_exists(&m, 0).await, "missing handler");

  R::sleep(Duration::from_millis(20)).await;

  assert!(!ack_handler_exists(&m, 0).await, "non-reaped handler");
}

/// Unit test to test the set ack handler functionality
pub async fn set_ack_handler<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  m1.set_ack_handler::<_, R>(0, Duration::from_millis(10), |_1, _2| {
    Box::pin(async move {})
  });

  assert!(ack_handler_exists(&m1, 0).await, "missing handler");

  R::sleep(Duration::from_millis(20)).await;

  assert!(!ack_handler_exists(&m1, 0).await, "non-reaped handler");
}

pub async fn invoke_ack_handler<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  // Does nothing
  m1.invoke_ack_handler(
    Ack {
      seq_no: 0,
      payload: Default::default(),
    },
    Instant::now(),
  )
  .await;

  let b = Arc::new(AtomicBool::new(false));
  let b1 = b.clone();
  m1.set_ack_handler::<_, R>(0, Duration::from_millis(10), |_, _| {
    Box::pin(async move {
      b1.store(true, Ordering::SeqCst);
    })
  });

  // Should set b
  m1.invoke_ack_handler(
    Ack {
      seq_no: 0,
      payload: Default::default(),
    },
    Instant::now(),
  )
  .await;
  assert!(b.load(Ordering::SeqCst), "b not set");
}

/// Unit test to test the invoke ack handler channel ack functionality
pub async fn invoke_ack_handler_channel_ack<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m = AckManager::new();

  let ack = Ack {
    seq_no: 0,
    payload: Bytes::from_static(&[0, 0, 0]),
  };

  // Does nothing
  m.invoke_ack_handler(ack.clone(), Instant::now()).await;

  let (ack_tx, ack_rx) = async_channel::bounded(1);
  let (nack_tx, nack_rx) = async_channel::bounded(1);
  m.set_probe_channels::<R>(
    0,
    ack_tx,
    Some(nack_tx),
    Instant::now(),
    Duration::from_millis(10),
  );

  // Should send message
  m.invoke_ack_handler(ack.clone(), Instant::now()).await;

  loop {
    futures::select! {
      v = ack_rx.recv().fuse() => {
        let v = v.unwrap();
        assert!(v.complete, "bad value");
        assert_eq!(v.payload, ack.payload, "wrong payload. expected: {:?}; actual: {:?}", ack.payload, v.payload);
        break;
      },
      res = nack_rx.recv().fuse() => {
        if res.is_ok() {
          panic!("should not get a nack")
        }
      },
      default => {
        panic!("message not sent");
      }
    }
  }

  assert!(!ack_handler_exists(&m, 0).await, "non-reaped handler");
}

/// Unit test to test the invoke ack handler channel nack functionality
pub async fn invoke_ack_handler_channel_nack<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  let m1 = AckManager::new();

  // Does nothing
  let nack = Nack { seq_no: 0 };
  m1.invoke_nack_handler(nack).await;

  let (ack_tx, ack_rx) = async_channel::bounded(1);
  let (nack_tx, nack_rx) = async_channel::bounded(1);
  m1.set_probe_channels::<R>(
    0,
    ack_tx,
    Some(nack_tx),
    Instant::now(),
    Duration::from_millis(10),
  );

  // Should send message
  m1.invoke_nack_handler(nack).await;

  futures::select! {
    _ = ack_rx.recv().fuse() => panic!("should not get an ack"),
    _ = nack_rx.recv().fuse() => {
      // Good
    },
    default => {
      panic!("message not sent");
    }
  }

  // Getting a nack doesn't reap the handler so that we can still forward
  // an ack up to the reap time, if we get one.
  assert!(
    ack_handler_exists(&m1, 0).await,
    "handler should not be reaped"
  );

  let ack = Ack {
    seq_no: 0,
    payload: Bytes::from_static(&[0, 0, 0]),
  };
  m1.invoke_ack_handler(ack.clone(), Instant::now()).await;

  loop {
    futures::select! {
      v = ack_rx.recv().fuse() => {
        let v = v.unwrap();
        assert!(v.complete, "bad value");
        assert_eq!(v.payload, ack.payload, "wrong payload. expected: {:?}; actual: {:?}", ack.payload, v.payload);
        break;
      },
      res = nack_rx.recv().fuse() => {
        if res.is_ok() {
          panic!("should not get a nack")
        }
      },
      default => {
        panic!("message not sent");
      }
    }
  }

  assert!(!ack_handler_exists(&m1, 0).await, "non-reaped handler");
}

pub async fn test_alive_node_new_node<T, R>(
  _t1: T,
  _t1_opts: Options,
  _test_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  todo!()
}

pub async fn test_alive_node_suspect_node() {
  todo!()
}

pub async fn test_alive_node_idempotent() {
  todo!()
}

pub async fn test_alive_node_change_meta() {
  todo!()
}

/// Unit test to test the alive node refute functionality
pub async fn alive_node_refute<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, true).await;

  // Clear queue
  m.inner.broadcast.reset().await;

  // Conflicting alive
  let a = Alive {
    incarnation: 2,
    meta: Bytes::from_static(b"foo"),
    node: m.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(m.local_id()).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert!(n.meta.is_empty(), "meta should still be empty");
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Should be alive msg
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the alive node conflict functionality
pub async fn alive_node_conflict<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: test_node1.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m.alive_node(a, None, true).await;

  // Clear queue
  m.inner.broadcast.reset().await;

  // Conflicting alive
  let test_node2 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.2:9000".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 2,
    meta: Bytes::from_static(b"foo"),
    node: test_node2.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert!(n.meta.is_empty(), "meta should still be empty");
    assert_eq!(n.id, test_node_id, "id should not be update");
    assert_eq!(&n.addr, test_node1.address(), "addr should not be updated");
  }

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected 0 queued message: {num}");

  // Change the node to dead
  let d = Dead {
    incarnation: 2,
    node: test_node_id.clone(),
    from: m.local_id().cheap_clone(),
  };
  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut *members, d).await.unwrap();
  }

  m.inner.broadcast.reset().await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Dead, "should be dead");
  }

  R::sleep(m.inner.opts.dead_node_reclaim_time).await;

  // New alive node
  let a = Alive {
    incarnation: 3,
    meta: Bytes::from_static(b"foo"),
    node: test_node2.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  {
    let nodes = m.inner.nodes.read().await;
    let n = nodes.get_state(&test_node_id).unwrap();
    assert_eq!(n.state, State::Alive, "should still be alive");
    assert_eq!(n.meta, Bytes::from_static(b"foo"), "meta should be updated");
    assert_eq!(&n.addr, test_node2.address(), "addr should be updated");
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node no node functionality
pub async fn suspect_node_no_node<T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let s = Suspect {
    incarnation: 1,
    node: test_node_id,
    from: m.local_id().cheap_clone(),
  };

  m.suspect_node(s).await.unwrap();

  {
    let nodes = m.inner.nodes.read().await.node_map.len();
    assert_eq!(nodes, 0, "don't expect nodes");
  }

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node functionality
pub async fn suspect_node<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts
      .with_probe_interval(Duration::from_millis(1))
      .with_suspicion_mult(1),
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: test_node1.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let s = Suspect {
    incarnation: 1,
    node: test_node_id.clone(),
    from: m.local_id().cheap_clone(),
  };

  m.suspect_node(s).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Suspect, "bad state");

  let change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Instant::now() - change <= Duration::from_secs(1),
    "bad change delta"
  );

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a suspect message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Suspect(_)), "bad message: {msg:?}");

  // Wait for the timeout
  R::sleep(Duration::from_millis(10)).await;

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Dead, "bad state");

  let new_change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Instant::now() - new_change <= Duration::from_secs(1),
    "bad change delta"
  );

  assert!(
    new_change.checked_duration_since(change).is_some(),
    "should increment time"
  );

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Check its a suspect message
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Dead(_)), "bad message: {msg:?}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node double suspect functionality
pub async fn suspect_node_double_suspect<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: test_node1.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: state.state_change.sub(Duration::from_secs(3600)),
      ..state.clone()
    };
  })
  .await;

  let s = Suspect {
    incarnation: 1,
    node: test_node_id.clone(),
    from: m.local_id().cheap_clone(),
  };

  m.suspect_node(s.clone()).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Suspect, "bad state");

  let change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert!(
    Instant::now() - change <= Duration::from_secs(1),
    "bad change delta"
  );

  // clear the broadcast queue
  m.inner.broadcast.reset().await;

  // suspect again
  m.suspect_node(s).await.unwrap();

  let new_change = m.get_node_state_change(&test_node_id).await.unwrap();
  assert_eq!(new_change, change, "unexpected change");

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected no queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node old suspect functionality
pub async fn suspect_node_old_suspect<A, T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let now = Instant::now();
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let test_node1 = Node::new(
    test_node_id.cheap_clone(),
    "127.0.0.1:8000".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: test_node1.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, false).await;

  m.change_node(&test_node_id, |state| {
    *state = LocalNodeState {
      state_change: now,
      ..state.clone()
    };
  })
  .await;

  // clear queue
  m.inner.broadcast.reset().await;

  let s = Suspect {
    incarnation: 1,
    node: test_node_id.clone(),
    from: m.local_id().cheap_clone(),
  };

  m.suspect_node(s.clone()).await.unwrap();

  let state = m.get_node_state(&test_node_id).await.unwrap();
  assert_eq!(state, State::Alive, "bad state");

  // check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 0, "expected 0 queued message: {num}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the suspect node refute functionality
pub async fn suspect_node_refute<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let a = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m.alive_node(a, None, true).await;

  // clear queue
  m.inner.broadcast.reset().await;

  // make sure health is in a good state
  let health = m.inner.awareness.get_health_score().await;
  assert_eq!(health, 0, "bad: {health}");

  let s = Suspect {
    incarnation: 1,
    node: m.local_id().cheap_clone(),
    from: m.local_id().cheap_clone(),
  };

  m.suspect_node(s).await.unwrap();

  let state = m.get_node_state(m.local_id()).await.unwrap();
  assert_eq!(state, State::Alive, "should still be alive");

  // Check a broad cast is queued
  let num = m.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only 1 queued message: {num}");

  // should be alive msg
  let broadcasts = m.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");

  // Health should have been dinged
  let health = m.inner.awareness.get_health_score().await;
  assert_eq!(health, 1, "bad: {health}");

  m.shutdown().await.unwrap();
}

/// Unit test to test the dead node no node functionality
pub async fn dead_node_no_node<T, R>(t1: T, t1_opts: Options, test_node_id: T::Id)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m: Memberlist<T> = get_memberlist(
    t1,
    VoidDelegate::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::default(),
    t1_opts,
  )
  .await
  .unwrap();

  let d = Dead {
    incarnation: 1,
    node: test_node_id,
    from: m.local_id().cheap_clone(),
  };

  {
    let mut members = m.inner.nodes.write().await;
    m.dead_node(&mut members, d).await.unwrap();
  }

  {
    let nodes = m.inner.nodes.read().await.node_map.len();
    assert_eq!(nodes, 0, "don't expect nodes");
  }

  m.shutdown().await.unwrap();
}

pub async fn test_dead_node_left() {
  todo!()
}

pub async fn test_dead_node() {
  todo!()
}

pub async fn test_dead_node_double() {
  todo!()
}

pub async fn test_dead_node_old_dead() {
  todo!()
}

pub async fn test_dead_node_alive_replay() {
  todo!()
}

pub async fn test_dead_node_refute() {
  todo!()
}

pub async fn test_merge_state() {
  todo!()
}

pub async fn test_gossip() {
  todo!()
}

pub async fn test_gossip_to_dead() {
  todo!()
}

pub async fn test_push_pull() {
  todo!()
}
