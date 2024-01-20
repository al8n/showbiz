use aead::{generic_array::GenericArray, Aead, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use atomic::{Atomic, Ordering};
use base64::Engine;
use bytes::{Buf, BufMut, BytesMut};
use crossbeam_skiplist::SkipSet;
use rand::Rng;
use serde::{Deserialize, Serialize};

use std::{
  iter::once,
  sync::{atomic::AtomicU64, Arc},
};

type Aes192Gcm = AesGcm<Aes192, U12>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SecurityError {
  #[error("security: unknown encryption version: {0}")]
  UnknownEncryptionAlgo(#[from] UnknownEncryptionAlgo),
  #[error("security: {0}")]
  AeadError(aead::Error),
  #[error("security: cannot decode empty payload")]
  EmptyPayload,
  #[error("security: payload is too small to decrypt")]
  SmallPayload,
  #[error("security: no installed keys could decrypt the message")]
  NoInstalledKeys,
  #[error("security: no primary key installed")]
  MissingPrimaryKey,
  #[error("security: remote state is encrypted and encryption is not configured")]
  NotConfigured,
  #[error("security: encryption is configured but remote state is not encrypted")]
  PlainRemoteState,
  #[error("security: secret key is not in the keyring")]
  SecretKeyNotFound,
  #[error("security: removing the primary key is not allowed")]
  RemovePrimaryKey,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UnknownEncryptionAlgo(u8);

impl core::fmt::Display for UnknownEncryptionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown encryption type {}", self.0)
  }
}

impl std::error::Error for UnknownEncryptionAlgo {}

#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum EncryptionAlgo {
  /// No encryption
  #[default]
  None = 0,

  /// AES-GCM 128, using PKCS7 padding
  PKCS7 = 1,
  /// AES-GCM 128, no padding. Padding not needed,
  NoPadding = 2,
}

impl EncryptionAlgo {
  pub const MAX: Self = Self::NoPadding;
  pub const MIN: Self = Self::None;

  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();

  pub fn from_u8(val: u8) -> Result<Self, UnknownEncryptionAlgo> {
    match val {
      0 => Ok(EncryptionAlgo::None),
      1 => Ok(EncryptionAlgo::PKCS7),
      2 => Ok(EncryptionAlgo::NoPadding),
      _ => Err(UnknownEncryptionAlgo(val)),
    }
  }

  pub fn is_none(&self) -> bool {
    *self == EncryptionAlgo::None
  }

  pub(crate) const fn encrypted_length(&self, inp: usize) -> usize {
    match self {
      Self::None => EncryptionAlgo::SIZE + NONCE_SIZE + inp + TAG_SIZE,
      Self::PKCS7 => {
        // Determine the padding size
        let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

        // Sum the extra parts to get total size
        EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE + inp + padding + TAG_SIZE
      }
      Self::NoPadding => {
        EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE + inp + TAG_SIZE
      }
    }
  }

  pub(crate) fn header(&self, len: usize) -> BytesMut {
    let enc_len = self.encrypted_length(len);
    let enc_len_bytes = (enc_len as u32).to_be_bytes();
    let mut buf = BytesMut::with_capacity(ENCODE_HEADER_SIZE + enc_len);
    // Write the encrypt byte
    buf.put_slice(&[
      MessageType::Encrypt as u8,
      0,
      0,
      0,
      enc_len_bytes[0],
      enc_len_bytes[1],
      enc_len_bytes[2],
      enc_len_bytes[3], // message length
    ]);
    buf
  }
}

/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
}

impl Serialize for SecretKey {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    if serializer.is_human_readable() {
      base64::engine::general_purpose::STANDARD
        .encode(self)
        .serialize(serializer)
    } else {
      serializer.serialize_bytes(self)
    }
  }
}

impl<'de> Deserialize<'de> for SecretKey {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    macro_rules! parse {
      ($key:ident) => {{
        match $key.len() {
          16 => Ok(Self::Aes128($key.try_into().unwrap())),
          24 => Ok(Self::Aes192($key.try_into().unwrap())),
          32 => Ok(Self::Aes256($key.try_into().unwrap())),
          _ => Err(<D::Error as serde::de::Error>::custom(
            "invalid secret key length",
          )),
        }
      }};
    }

    if deserializer.is_human_readable() {
      <String as Deserialize<'de>>::deserialize(deserializer).and_then(|val| {
        base64::engine::general_purpose::STANDARD
          .decode(val)
          .map_err(serde::de::Error::custom)
          .and_then(|key| parse!(key))
      })
    } else {
      <Vec<u8> as Deserialize<'de>>::deserialize(deserializer).and_then(|val| parse!(val))
    }
  }
}

impl core::hash::Hash for SecretKey {
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl core::borrow::Borrow<[u8]> for SecretKey {
  fn borrow(&self) -> &[u8] {
    self.as_ref()
  }
}

impl PartialEq<[u8]> for SecretKey {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref() == other
  }
}

impl core::ops::Deref for SecretKey {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl core::ops::DerefMut for SecretKey {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl SecretKey {
  /// Returns the size of the key in bytes
  #[inline]
  pub const fn len(&self) -> usize {
    match self {
      Self::Aes128(_) => 16,
      Self::Aes192(_) => 24,
      Self::Aes256(_) => 32,
    }
  }

  /// Returns true if the key is empty
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.len() == 0
  }

  #[inline]
  pub fn to_vec(&self) -> Vec<u8> {
    self.as_ref().to_vec()
  }
}

impl From<[u8; 16]> for SecretKey {
  fn from(k: [u8; 16]) -> Self {
    Self::Aes128(k)
  }
}

impl From<[u8; 24]> for SecretKey {
  fn from(k: [u8; 24]) -> Self {
    Self::Aes192(k)
  }
}

impl From<[u8; 32]> for SecretKey {
  fn from(k: [u8; 32]) -> Self {
    Self::Aes256(k)
  }
}

impl TryFrom<&[u8]> for SecretKey {
  type Error = String;

  fn try_from(k: &[u8]) -> Result<Self, Self::Error> {
    match k.len() {
      16 => Ok(Self::Aes128(k.try_into().unwrap())),
      24 => Ok(Self::Aes192(k.try_into().unwrap())),
      32 => Ok(Self::Aes256(k.try_into().unwrap())),
      x => Err(format!(
        "invalid key size: {}, secret key size must be 16, 24 or 32 bytes",
        x
      )),
    }
  }
}

impl AsRef<[u8]> for SecretKey {
  fn as_ref(&self) -> &[u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl AsMut<[u8]> for SecretKey {
  fn as_mut(&mut self) -> &mut [u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

#[derive(Debug, Serialize, Deserialize)]
struct SecretKeyringInner {
  #[serde(with = "primary_key_serde")]
  primary_key: Atomic<SecretKey>,
  update_sequence: AtomicU64,
  #[serde(with = "keys_serde")]
  keys: SkipSet<SecretKey>,
}

mod primary_key_serde {
  use super::SecretKey;
  use atomic::{Atomic, Ordering};
  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  pub fn serialize<S>(pk: &Atomic<SecretKey>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    pk.load(Ordering::Relaxed).serialize(serializer)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Atomic<SecretKey>, D::Error>
  where
    D: Deserializer<'de>,
  {
    <SecretKey as Deserialize<'_>>::deserialize(deserializer).map(Atomic::new)
  }
}

mod keys_serde {
  use crossbeam_skiplist::SkipSet;
  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  use super::SecretKey;

  pub fn serialize<S>(keys: &SkipSet<SecretKey>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    keys
      .iter()
      .map(|ent| *ent.value())
      .collect::<Vec<_>>()
      .serialize(serializer)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<SkipSet<SecretKey>, D::Error>
  where
    D: Deserializer<'de>,
  {
    <Vec<SecretKey> as Deserialize<'_>>::deserialize(deserializer)
      .map(|keys| keys.into_iter().collect::<SkipSet<_>>())
  }
}

/// A lock-free and thread-safe container for a set of encryption keys.
/// The keyring contains all key data used internally by showbiz.
///
/// If creating a keyring with multiple keys, one key must be designated
/// primary by passing it as the primaryKey. If the primaryKey does not exist in
/// the list of secondary keys, it will be automatically added at position 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SecretKeyring {
  inner: Arc<SecretKeyringInner>,
}

impl SecretKeyring {
  /// Constructs a new container for a primary key. The
  /// keyring contains all key data used internally by showbiz.
  ///
  /// If only a primary key is passed, then it will be automatically added to the
  /// keyring.
  ///
  /// A key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn new(primary_key: SecretKey) -> Self {
    Self {
      inner: Arc::new(SecretKeyringInner {
        primary_key: Atomic::new(primary_key),
        update_sequence: AtomicU64::new(0),
        keys: once(primary_key).collect(),
      }),
    }
  }

  /// Constructs a new container for a set of encryption keys. The
  /// keyring contains all key data used internally by showbiz.
  ///
  /// If only a primary key is passed, then it will be automatically added to the
  /// keyring. If creating a keyring with multiple keys, one key must be designated
  /// primary by passing it as the primaryKey. If the primaryKey does not exist in
  /// the list of secondary keys, it will be automatically added.
  ///
  /// A key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn with_keys(primary_key: SecretKey, keys: Vec<SecretKey>) -> Self {
    if !keys.is_empty() {
      return Self {
        inner: Arc::new(SecretKeyringInner {
          primary_key: Atomic::new(primary_key),
          update_sequence: AtomicU64::new(0),
          keys: once(primary_key).chain(keys).collect(),
        }),
      };
    }

    Self {
      inner: Arc::new(SecretKeyringInner {
        primary_key: Atomic::new(primary_key),
        update_sequence: AtomicU64::new(0),
        keys: once(primary_key).collect(),
      }),
    }
  }

  /// Returns the key on the ring at position 0. This is the key used
  /// for encrypting messages, and is the first key tried for decrypting messages.
  #[inline]
  pub fn primary_key(&self) -> SecretKey {
    self.inner.primary_key.load(Ordering::SeqCst)
  }

  /// Drops a key from the keyring. This will return an error if the key
  /// requested for removal is currently at position 0 (primary key).
  #[inline]
  pub fn remove(&self, key: &[u8]) -> Result<(), SecurityError> {
    if &self.primary_key() == key {
      return Err(SecurityError::RemovePrimaryKey);
    }
    self.inner.keys.remove(key);
    Ok(())
  }

  /// Install a new key on the ring. Adding a key to the ring will make
  /// it available for use in decryption. If the key already exists on the ring,
  /// this function will just return noop.
  ///
  /// key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn insert(&self, key: SecretKey) {
    self.inner.keys.insert(key);
  }

  /// Changes the key used to encrypt messages. This is the only key used to
  /// encrypt messages, so peers should know this key before this method is called.
  #[inline]
  pub fn use_key(&self, key_data: &[u8]) -> Result<(), SecurityError> {
    if key_data == self.primary_key().as_ref() {
      return Ok(());
    }

    // Try to find the key to set as primary
    let Some(entry) = self.inner.keys.get(key_data) else {
      return Err(SecurityError::SecretKeyNotFound);
    };

    let key = *entry.value();

    // Increment the sequence number for the new update
    let new_sequence = self.inner.update_sequence.fetch_add(1, Ordering::AcqRel) + 1;

    // Try to update the primary key
    let mut seq = self.inner.update_sequence.load(Ordering::Acquire);
    loop {
      if new_sequence < seq {
        return Ok(());
      }

      match self.inner.update_sequence.compare_exchange_weak(
        seq,
        new_sequence,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => {
          self.inner.primary_key.store(key, Ordering::Release);
          return Ok(());
        }
        Err(current_seq) => {
          seq = current_seq;
        }
      }
    }
  }

  /// Returns the current set of keys on the ring.
  #[inline]
  pub fn keys(&self) -> impl Iterator<Item = SecretKey> + '_ {
    let primary_key = self.primary_key();

    // we must promise the first key is the primary key
    // so that when decrypt messages, we can try the primary key first
    once(primary_key).chain(
      self
        .inner
        .keys
        .iter()
        .filter_map(move |entry| Some(*entry.value()).filter(|key| key != &primary_key)),
    )
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.inner.keys.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.inner.keys.is_empty()
  }
}

pub(crate) const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
pub(crate) const BLOCK_SIZE: usize = 16;
const ENCRYPT_HEADER_PADDING: usize = 3;

// pkcs7encode is used to pad a byte buffer to a specific block size using
// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
#[inline]
pub(crate) fn pkcs7encode(buf: &mut impl BufMut, buf_len: usize, ignore: usize, block_size: usize) {
  let n = buf_len - ignore;
  let more = block_size - (n % block_size);
  buf.put_bytes(more as u8, more);
}

// pkcs7decode is used to decode a buffer that has been padded
#[inline]
fn pkcs7decode(buf: &mut BytesMut) {
  if buf.is_empty() {
    panic!("Cannot decode a PKCS7 buffer of zero length");
  }
  let n = buf.len();
  let last = buf[n - 1];
  let n = n - (last as usize);
  buf.truncate(n);
}

pub(crate) fn encrypt_overhead(vsn: EncryptionAlgo) -> usize {
  match vsn {
    EncryptionAlgo::PKCS7 => 45, // Version: 1, IV: 12, Padding: 16, Tag: 16
    EncryptionAlgo::NoPadding => 29, // Version: 1, IV: 12, Tag: 16
    EncryptionAlgo::None => unreachable!(),
  }
}

macro_rules! bail {
  (enum $ty:ident: $key:ident { $($var:ident($algo: ident)),+ $(,)? } -> $fn:expr) => {
    match $key {
      $($ty::$var(key) => $fn($algo::new(GenericArray::from_slice(key)))),*
    }
  };
  (enum $ty:ident: &$key:ident { $($var:ident($algo: ident)),+ $(,)? } -> $fn:expr) => {
    match $key {
      $($ty::$var(key) => $fn($algo::new(GenericArray::from_slice(&key)))),*
    }
  };
}

#[inline]
fn decrypt_message(key: &SecretKey, msg: &mut BytesMut, data: &[u8]) -> Result<(), SecurityError> {
  bail! {
    enum SecretKey: key {
      Aes128(Aes128Gcm),
      Aes192(Aes192Gcm),
      Aes256(Aes256Gcm),
    } -> |algo| decrypt_message_in(algo, msg, data)
  }
}

#[inline]
fn encrypt_payload_in<A: AeadInPlace + Aead>(
  gcm: A,
  vsn: EncryptionAlgo,
  msg: &[u8],
  data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  let offset = dst.len();
  dst.reserve(vsn.encrypted_length(msg.len()));

  // Write the encryption version
  dst.put_slice(&[
    vsn as u8, 0, 0, 0, // 3-bytes padding
  ]);
  // Add a random nonce
  let mut nonce = [0u8; NONCE_SIZE];
  rand::thread_rng().fill(&mut nonce);
  dst.put_slice(&nonce);
  let after_nonce = offset + NONCE_SIZE + EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING;
  dst.put_slice(msg);

  // Encrypt message using GCM
  let nonce = GenericArray::from_slice(&nonce);
  // Ensure we are correctly padded (now, only for PKCS7)
  match vsn {
    EncryptionAlgo::None => Ok(()),
    EncryptionAlgo::PKCS7 => {
      let buf_len = dst.len();
      pkcs7encode(
        dst,
        buf_len,
        offset + EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE,
        BLOCK_SIZE,
      );
      let mut bytes = dst.split_off(after_nonce);
      gcm
        .encrypt_in_place(nonce, data, &mut bytes)
        .map(|_| {
          dst.unsplit(bytes);
        })
        .map_err(SecurityError::AeadError)
    }
    EncryptionAlgo::NoPadding => {
      let mut bytes = dst.split_off(after_nonce);
      gcm
        .encrypt_in_place(nonce, data, &mut bytes)
        .map(|_| {
          dst.unsplit(bytes);
        })
        .map_err(SecurityError::AeadError)
    }
  }
}

#[inline]
fn decrypt_message_in<A: Aead + AeadInPlace>(
  gcm: A,
  msg: &mut BytesMut,
  data: &[u8],
) -> Result<(), SecurityError> {
  // Decrypt the message
  let mut ciphertext = msg.split_off(EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE);
  let nonce = GenericArray::from_slice(
    &msg[EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING
      ..EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE],
  );

  gcm
    .decrypt_in_place(nonce, data, &mut ciphertext)
    .map_err(SecurityError::AeadError)?;
  msg.unsplit(ciphertext);
  msg.advance(EncryptionAlgo::SIZE + ENCRYPT_HEADER_PADDING + NONCE_SIZE);
  Ok(())
}

pub(crate) fn encrypt_payload(
  key: &SecretKey,
  vsn: EncryptionAlgo,
  msg: &[u8],
  data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  bail! {
    enum SecretKey: key {
      Aes128(Aes128Gcm),
      Aes192(Aes192Gcm),
      Aes256(Aes256Gcm),
    } -> |algo| encrypt_payload_in(algo, vsn, msg, data, dst)
  }
}

impl SecretKeyring {
  pub(crate) fn encrypt_payload(
    &self,
    primary_key: SecretKey,
    vsn: EncryptionAlgo,
    msg: &[u8],
    data: &[u8],
    dst: &mut BytesMut,
  ) -> Result<(), SecurityError> {
    encrypt_payload(&primary_key, vsn, msg, data, dst)
  }

  pub(crate) fn decrypt_payload(
    &self,
    msg: &mut BytesMut,
    data: &[u8],
  ) -> Result<(), SecurityError> {
    // Ensure we have at least one byte
    if msg.is_empty() {
      return Err(SecurityError::EmptyPayload);
    }

    // Verify the version
    let vsn = EncryptionAlgo::from_u8(msg[0])?;

    // Ensure the length is sane
    if msg.len() < vsn.encrypted_length(0) {
      return Err(SecurityError::SmallPayload);
    }

    decrypt_payload(self.keys(), msg, data, vsn)
  }
}

#[inline]
fn decrypt_payload(
  keys: impl Iterator<Item = SecretKey>,
  msg: &mut BytesMut,
  data: &[u8],
  vsn: EncryptionAlgo,
) -> Result<(), SecurityError> {
  for key in keys {
    match decrypt_message(&key, msg, data) {
      Ok(_) => {
        // Remove the PKCS7 padding for vsn 0
        if vsn == EncryptionAlgo::PKCS7 {
          pkcs7decode(msg);
        }
        return Ok(());
      }
      Err(_) => continue,
    }
  }

  Err(SecurityError::NoInstalledKeys)
}

pub(crate) fn append_bytes(first: &[u8], second: &[u8]) -> Vec<u8> {
  let has_first = !first.is_empty();
  let has_second = !second.is_empty();

  match (has_first, has_second) {
    (true, true) => [first, second].concat(),
    (true, false) => first.to_vec(),
    (false, true) => second.to_vec(),
    (false, false) => Vec::new(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";
    let mut encrypted = BytesMut::new();
    encrypt_payload(&k1, vsn, plain_text, extra, &mut encrypted).unwrap();

    let exp_len = vsn.encrypted_length(plain_text.len());
    assert_eq!(encrypted.len(), exp_len);

    decrypt_payload([k1].into_iter(), &mut encrypted, extra, vsn).unwrap();
    assert_eq!(&encrypted[EncryptionAlgo::SIZE + NONCE_SIZE..], plain_text);
  }

  #[test]
  fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(EncryptionAlgo::PKCS7);
  }

  #[test]
  fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(EncryptionAlgo::NoPadding);
  }

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];

  #[test]
  fn test_primary_only() {
    let keyring = SecretKeyring::new(TEST_KEYS[1]);
    assert_eq!(keyring.keys().collect::<Vec<_>>().len(), 1);
  }

  #[test]
  fn test_get_primary_key() {
    let keyring = SecretKeyring::with_keys(TEST_KEYS[1], TEST_KEYS.to_vec());
    assert_eq!(keyring.primary_key().as_ref(), TEST_KEYS[1].as_ref());
  }

  #[test]
  fn test_insert_remove_use() {
    let keyring = SecretKeyring::new(TEST_KEYS[1]);

    // Use non-existent key throws error
    keyring.use_key(&TEST_KEYS[2]).unwrap_err();

    // Add key to ring
    keyring.insert(TEST_KEYS[2]);
    assert_eq!(keyring.len(), 2);
    assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[1]);

    // Use key that exists should succeed
    keyring.use_key(&TEST_KEYS[2]).unwrap();
    assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[2]);

    let primary_key = keyring.primary_key();
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keyring.remove(&TEST_KEYS[2]).unwrap_err();

    // Removing non-primary key should succeed
    keyring.remove(&TEST_KEYS[1]).unwrap();
    assert_eq!(keyring.len(), 1);
  }
}
