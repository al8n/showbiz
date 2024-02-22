use memberlist::{
  transport::{resolver::socket_addr::SocketAddrResolver, Node, Transport},
  Options,
};
use memberlist_core::{
  tests::{memberlist::*, next_socket_addr_v4, state::*},
  transport::Lpe,
};
use memberlist_net::{NetTransport, NetTransportOptions};
use smol_str::SmolStr;

#[cfg(feature = "encryption")]
use memberlist_net::security::SecretKey;

#[cfg(feature = "encryption")]
pub const TEST_KEYS: &[SecretKey] = &[
  SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
  SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
  SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
];

#[path = "net/probe.rs"]
mod probe;

#[path = "net/probe_node.rs"]
mod probe_node;

#[path = "net/probe_node_dogpile.rs"]
mod probe_node_dogpile;

#[path = "net/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "net/probe_node_suspect.rs"]
mod probe_node_suspect;

#[path = "net/probe_node_awareness_missed_nack.rs"]
mod probe_node_awareness_missed_nack;

#[path = "net/probe_node_awareness_improved.rs"]
mod probe_node_awareness_improved;

#[path = "net/probe_node_awareness_degraded.rs"]
mod probe_node_awareness_degraded;

#[path = "net/ping.rs"]
mod ping;

#[path = "net/reset_nodes.rs"]
mod reset_nodes;

#[path = "net/alive_node_new_node.rs"]
mod alive_node_new_node;

#[path = "net/alive_node_conflict.rs"]
mod alive_node_conflict;

#[path = "net/alive_node_suspect_node.rs"]
mod alive_node_suspect_node;

#[path = "net/alive_node_idempotent.rs"]
mod alive_node_idempotent;

#[path = "net/alive_node_change_meta.rs"]
mod alive_node_change_meta;

#[path = "net/alive_node_refute.rs"]
mod alive_node_refute;

#[path = "net/suspect_node_no_node.rs"]
mod suspect_node_no_node;

#[path = "net/suspect_node.rs"]
mod suspect_node;

#[path = "net/suspect_node_double_suspect.rs"]
mod suspect_node_double_suspect;

#[path = "net/suspect_node_refute.rs"]
mod suspect_node_refute;

#[path = "net/dead_node_no_node.rs"]
mod dead_node_no_node;

#[path = "net/dead_node_left.rs"]
mod dead_node_left;

#[path = "net/dead_node.rs"]
mod dead_node;

#[path = "net/dead_node_double.rs"]
mod dead_node_double;

#[path = "net/dead_node_old_dead.rs"]
mod dead_node_old_dead;

#[path = "net/dead_node_alive_replay.rs"]
mod dead_node_alive_replay;

#[path = "net/dead_node_refute.rs"]
mod dead_node_refute;

#[path = "net/merge_state.rs"]
mod merge_state;

#[path = "net/gossip.rs"]
mod gossip;

#[path = "net/gossip_to_dead.rs"]
mod gossip_to_dead;

#[path = "net/push_pull.rs"]
mod push_pull;

// ------- memberlist tests ----------

#[path = "net/join.rs"]
mod join;

#[path = "net/join_with_labels.rs"]
mod join_with_labels;

#[path = "net/join_with_labels_and_encryption.rs"]
#[cfg(feature = "encryption")]
mod join_with_labels_and_encryption;

#[path = "net/join_with_labels_and_compression.rs"]
#[cfg(feature = "compression")]
mod join_with_labels_and_compression;

#[path = "net/join_cancel.rs"]
mod join_cancel;

#[path = "net/join_cancel_passive.rs"]
mod join_cancel_passive;

#[path = "net/join_shutdown.rs"]
mod join_shutdown;

#[path = "net/leave.rs"]
mod leave;

#[path = "net/conflict_delegate.rs"]
mod conflict_delegate;

#[path = "net/create_shutdown.rs"]
mod create_shutdown;

#[path = "net/create.rs"]
mod create;

#[path = "net/send.rs"]
mod send;

#[path = "net/user_data.rs"]
mod user_data;

#[path = "net/node_delegate_meta.rs"]
mod node_delegate_meta;

#[path = "net/node_delegate_meta_update.rs"]
mod node_delegate_meta_update;

#[path = "net/ping_delegate.rs"]
mod ping_delegate;
