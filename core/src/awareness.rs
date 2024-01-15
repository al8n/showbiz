use std::{sync::Arc, time::Duration};

#[derive(Debug)]
pub(crate) struct Inner {
  /// max is the upper threshold for the timeout scale (the score will be
  /// constrained to be from 0 <= score < max).
  max: isize,
  /// score is the current awareness score. Lower values are healthier and
  /// zero is the minimum value.
  score: isize,
}

/// Manages a simple metric for tracking the estimated health of the
/// local node. Health is primary the node's ability to respond in the soft
/// real-time manner required for correct health checking of other nodes in the
/// cluster.
#[derive(Debug, Clone)]
pub(crate) struct Awareness {
  #[cfg(feature = "metrics")]
  id: crate::types::NodeId,
  pub(crate) inner: Arc<parking_lot::RwLock<Inner>>,
  #[cfg(feature = "metrics")]
  pub(crate) metric_labels: Arc<Vec<metrics::Label>>,
}

impl Awareness {
  /// Returns a new awareness object.
  pub(crate) fn new(
    max: isize,
    #[cfg(feature = "metrics")] metric_labels: Arc<Vec<metrics::Label>>,
    #[cfg(feature = "metrics")] id: crate::types::NodeId,
  ) -> Self {
    Self {
      inner: Arc::new(parking_lot::RwLock::new(Inner { max, score: 0 })),
      #[cfg(feature = "metrics")]
      metric_labels,
      #[cfg(feature = "metrics")]
      id,
    }
  }

  /// Takes the given delta and applies it to the score in a thread-safe
  /// manner. It also enforces a floor of zero and a max of max, so deltas may not
  /// change the overall score if it's railed at one of the extremes.
  pub(crate) fn apply_delta(&self, delta: isize) {
    let (_initial, _fnl) = {
      let mut inner = self.inner.write();
      let initial = inner.score;
      inner.score += delta;
      if inner.score < 0 {
        inner.score = 0;
      } else if inner.score > inner.max - 1 {
        inner.score = inner.max - 1;
      }
      (initial, inner.score)
    };

    #[cfg(feature = "metrics")]
    {
      static HEALTH_GAUGE: std::sync::Once = std::sync::Once::new();

      if _initial != _fnl {
        // HEALTH_GAUGE.call_once(|| {
        //   metrics::register_gauge!("showbiz.health.score", "node" => self.id.to_string());
        //   metrics::describe_gauge!("showbiz.health.score", "the health score of the local node");
        // });
        metrics::gauge!(
          "showbiz.health.score",
          self.metric_labels.iter()
        ).set(_fnl as f64);
      }
    }
  }

  /// Returns the raw health score.
  pub(crate) fn get_health_score(&self) -> isize {
    self.inner.read().score
  }

  /// Takes the given duration and scales it based on the current
  /// score. Less healthyness will lead to longer timeouts.
  pub(crate) fn scale_timeout(&self, timeout: Duration) -> Duration {
    let score = self.inner.read().score;
    timeout * ((score + 1) as u32)
  }
}

#[test]
#[cfg(test)]
fn test_awareness() {
  use std::net::SocketAddr;

  use crate::types::{Name, NodeId};

  let cases = vec![
    (0, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
    (-10, 0, Duration::from_secs(1)),
    (1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (10, 7, Duration::from_secs(8)),
    (-1, 6, Duration::from_secs(7)),
    (-1, 5, Duration::from_secs(6)),
    (-1, 4, Duration::from_secs(5)),
    (-1, 3, Duration::from_secs(4)),
    (-1, 2, Duration::from_secs(3)),
    (-1, 1, Duration::from_secs(2)),
    (-1, 0, Duration::from_secs(1)),
    (-1, 0, Duration::from_secs(1)),
  ];
  let a = Awareness::new(
    8,
    #[cfg(feature = "metrics")]
    Arc::new(vec![]),
    #[cfg(feature = "metrics")]
    NodeId::new(
      Name::from_static("1").unwrap(),
      SocketAddr::from(([127, 0, 0, 1], 0)),
    ),
  );
  for (delta, score, timeout) in cases {
    a.apply_delta(delta);
    assert_eq!(a.get_health_score(), score);
    assert_eq!(a.scale_timeout(Duration::from_secs(1)), timeout);
  }
}
