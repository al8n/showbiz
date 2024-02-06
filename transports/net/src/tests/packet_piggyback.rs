use memberlist_core::transport::{tests::send_packet_piggyback, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn packet_piggyback<S, R>(s: S, kind: AddressKind) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_packet_piggyback");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(kind.next())
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_encrypted(Some(pk))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  send_packet_piggyback(trans, client, |mut src| {
    let tag = src.get_u8();
    assert_eq!(tag, Message::<SmolStr, SocketAddr>::COMPOUND_TAG);
    let len = src.get_u8();
    assert_eq!(len, 2);

    let m1_len = NetworkEndian::read_u16(&src) as usize;
    src.advance(2);
    let m1 = src.split_to(m1_len);
    let (_, m1) = Message::decode(&m1)?;

    let m2_len = NetworkEndian::read_u16(&src) as usize;
    src.advance(2);
    let m2 = src.split_to(m2_len);
    let (_, m2) = Message::decode(&m2)?;
    Ok([m1, m2])
  })
  .await?;
  Ok(())
}
