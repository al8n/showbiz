use super::*;

use bytes::Bytes;
use memberlist_core::types::LabelBufMutExt;

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  S: StreamLayer<Runtime = R>,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  #[cfg(feature = "encryption")]
  fn enable_promised_encryption(&self) -> bool {
    self.encryptor.is_some()
      && self.opts.gossip_verify_outgoing
      && !S::is_secure()
      && self.opts.encryption_algo.is_some()
  }

  pub(crate) async fn send_by_promised(
    &self,
    mut conn: Deadline<&mut S::Stream, R::Instant>,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    let buf = self
      .send_by_promised_without_compression_and_encryption(&self.opts.label, msg)
      .await?;

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    let buf = self
      .send_by_promised_with_compression_without_encryption(&self.opts.label, msg)
      .await?;

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    let buf = self
      .send_by_promised_with_encryption_without_compression(msg, &self.opts.label)
      .await?;

    #[cfg(all(feature = "compression", feature = "encryption"))]
    let buf = self
      .send_by_promised_with_compression_and_encryption(msg, &self.opts.label)
      .await?;

    let total_len = buf.len();
    conn
      .write_all::<R>(&buf)
      .await
      .map_err(ConnectionError::promised_write)?;

    conn
      .op
      .flush()
      .await
      .map_err(|e| ConnectionError::promised_write(e).into())
      .map(|_| {
        let peer_addr = conn.op.peer_addr();
        tracing::trace!(remote_addr = %peer_addr, total_len = total_len, sent = ?buf.as_ref(), "memberlist_net.promised");
        total_len
      })
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  fn compress_and_encrypt(
    compressor: &Compressor,
    encryption_algo: EncryptionAlgo,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let label_encoded_size = label.encoded_overhead();
    let encrypt_header = encryption_algo.encrypt_overhead();
    let total_len = label_encoded_size + encrypt_header + COMPRESS_HEADER + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    // add label header
    buf.add_label_header(label);
    // write encryption algo
    buf.put_u8(encryption_algo as u8);
    let encrypt_message_len_offset = buf.len();
    // write length placeholder
    buf.put_slice(&[0; MAX_MESSAGE_LEN_SIZE]);

    let nonce_start_offset = buf.len();
    let nonce = security::write_header(&mut buf);
    let nonce_stop_offset = buf.len();
    buf.resize(total_len, 0);

    let encoded_len =
      W::encode_message(msg, &mut buf[nonce_stop_offset..]).map_err(NetTransportError::Wire)?;

    let compressed =
      compressor.compress_into_bytes(&buf[nonce_stop_offset..nonce_stop_offset + encoded_len])?;
    let compressed_size = compressed.len();
    buf.truncate(nonce_stop_offset);
    buf.put_u8(*compressor as u8);
    let mut compress_size_buf = [0; MAX_MESSAGE_LEN_SIZE];
    NetworkEndian::write_u32(&mut compress_size_buf, compressed_size as u32);
    buf.put_slice(&compress_size_buf);
    buf.put_slice(&compressed);

    let mut dst = buf.split_off(nonce_stop_offset);
    security::encrypt(encryption_algo, pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        let buf_len = buf.len();
        // update actual data size
        NetworkEndian::write_u32(
          &mut buf[encrypt_message_len_offset..],
          (buf_len - nonce_start_offset) as u32,
        );
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "encryption")]
  fn encrypt_message(
    encryption_algo: EncryptionAlgo,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let label_encoded_size = label.encoded_overhead();
    let encrypt_header = encryption_algo.encrypt_overhead();
    let total_len = label_encoded_size + encrypt_header + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    // add label header
    buf.add_label_header(label);
    // write encryption algo
    buf.put_u8(encryption_algo as u8);
    let encrypt_message_len_offset = buf.len();
    // write length placeholder
    buf.put_slice(&[0; MAX_MESSAGE_LEN_SIZE]);

    let nonce_start_offset = buf.len();
    let nonce = security::write_header(&mut buf);
    let nonce_stop_offset = buf.len();
    buf.resize(total_len, 0);

    let written =
      W::encode_message(msg, &mut buf[nonce_stop_offset..]).map_err(NetTransportError::Wire)?;
    buf.truncate(nonce_stop_offset + written);

    let mut dst = buf.split_off(nonce_stop_offset);
    security::encrypt(encryption_algo, pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        let buf_len = buf.len();
        // write actual data size
        NetworkEndian::write_u32(
          &mut buf[encrypt_message_len_offset..encrypt_message_len_offset + MAX_MESSAGE_LEN_SIZE],
          (buf_len - nonce_start_offset) as u32,
        );
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "compression")]
  fn compress_message(
    compressor: Compressor,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let label_encoded_size = label.encoded_overhead();
    let mut buf = BytesMut::with_capacity(label_encoded_size + COMPRESS_HEADER + encoded_size);
    buf.add_label_header(label);
    buf.put_u8(compressor as u8);
    buf.put_slice(&[0; MAX_MESSAGE_LEN_SIZE]);

    buf.resize(label_encoded_size + COMPRESS_HEADER + encoded_size, 0);
    let data = W::encode_message(msg, &mut buf[label_encoded_size + COMPRESS_HEADER..])
      .map_err(NetTransportError::Wire)?;

    debug_assert_eq!(
      data, encoded_size,
      "actual encoded size {} does not match expected encoded size {}",
      data, encoded_size
    );

    let compressed =
      compressor.compress_into_bytes(&buf[label_encoded_size + COMPRESS_HEADER..])?;

    let data_len = compressed.len();
    buf.truncate(label_encoded_size + COMPRESS_HEADER);
    buf.put_slice(&compressed);
    NetworkEndian::write_u32(
      &mut buf[label_encoded_size + 1..label_encoded_size + COMPRESS_HEADER],
      data_len as u32,
    );

    Ok(buf)
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  async fn send_by_promised_with_compression_and_encryption(
    &self,
    msg: Message<I, A::ResolvedAddress>,
    stream_label: &Label,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let compression_enabled = self.opts.compressor.is_some();
    let encryption_enabled = self.enable_promised_encryption();

    if !compression_enabled && !encryption_enabled {
      return self
        .send_by_promised_without_compression_and_encryption(stream_label, msg)
        .await;
    }

    if compression_enabled && !encryption_enabled {
      return self
        .send_by_promised_with_compression_without_encryption(stream_label, msg)
        .await;
    }

    if !compression_enabled && encryption_enabled {
      return self
        .send_by_promised_with_encryption_without_compression(msg, stream_label)
        .await;
    }

    let encoded_size = W::encoded_len(&msg);
    let encryptor = self.encryptor.as_ref().unwrap();
    let encryption_algo = self.opts.encryption_algo.unwrap();
    let compressor = self.opts.compressor.unwrap();

    let buf = if encoded_size < self.opts.offload_size {
      let pk = encryptor.primary_key().await;
      Self::compress_and_encrypt(
        &compressor,
        encryption_algo,
        pk,
        stream_label,
        msg,
        encoded_size,
      )?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let pk = encryptor.primary_key().await;
      let stream_label = stream_label.cheap_clone();

      rayon::spawn(move || {
        if tx
          .send(Self::compress_and_encrypt(
            &compressor,
            encryption_algo,
            pk,
            &stream_label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(
            "memberlist_net.promised: failed to send computation task result back to main thread"
          );
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };
    Ok(buf.freeze())
  }

  #[cfg(feature = "encryption")]
  async fn send_by_promised_with_encryption_without_compression(
    &self,
    msg: Message<I, A::ResolvedAddress>,
    stream_label: &Label,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let enable_encryption = self.enable_promised_encryption();
    if !enable_encryption {
      return self
        .send_by_promised_without_compression_and_encryption(stream_label, msg)
        .await;
    }

    let enp = self.encryptor.as_ref().unwrap();
    let encryption_algo = self.opts.encryption_algo.unwrap();
    let encoded_size = W::encoded_len(&msg);
    let buf = if encoded_size < self.opts.offload_size {
      let pk = enp.primary_key().await;
      Self::encrypt_message(encryption_algo, pk, stream_label, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let pk = enp.primary_key().await;
      let stream_label = stream_label.cheap_clone();
      rayon::spawn(move || {
        if tx
          .send(Self::encrypt_message(
            encryption_algo,
            pk,
            &stream_label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(
            "memberlist_net.promised: failed to send computation task result back to main thread"
          );
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    Ok(buf.freeze())
  }

  #[cfg(feature = "compression")]
  async fn send_by_promised_with_compression_without_encryption(
    &self,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let compressor = match self.opts.compressor {
      Some(c) => c,
      None => {
        return self
          .send_by_promised_without_compression_and_encryption(label, msg)
          .await
      }
    };

    let encoded_size = W::encoded_len(&msg);

    let buf = if encoded_size < self.opts.offload_size {
      Self::compress_message(compressor, label, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let label = label.cheap_clone();
      rayon::spawn(move || {
        if tx
          .send(Self::compress_message(
            compressor,
            &label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(
            "memberlist_net.promised: failed to send computation task result back to main thread"
          );
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    Ok(buf.freeze())
  }

  async fn send_by_promised_without_compression_and_encryption(
    &self,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let label_encoded_size = label.encoded_overhead();
    let msg_encoded_size = W::encoded_len(&msg);
    let mut buf = BytesMut::with_capacity(label_encoded_size + msg_encoded_size);
    buf.add_label_header(label);
    buf.resize(label_encoded_size + msg_encoded_size, 0);
    let data =
      W::encode_message(msg, &mut buf[label_encoded_size..]).map_err(NetTransportError::Wire)?;
    let total_data = data + label_encoded_size;

    debug_assert_eq!(
      total_data,
      label_encoded_size + msg_encoded_size,
      "actual encoded size {} does not match expected encoded size {}",
      total_data,
      label_encoded_size + msg_encoded_size
    );

    Ok(buf.freeze())
  }
}
