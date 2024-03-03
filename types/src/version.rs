/// Unknown delegate version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid delegate version")]
pub struct UnknownDelegateVersion(u8);

/// Delegate version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(
    derive(Debug, Copy, Clone, Eq, PartialEq, Hash),
    repr(u8),
    non_exhaustive
  )
)]
#[non_exhaustive]
#[repr(u8)]
pub enum DelegateVersion {
  /// Version 0
  #[default]
  V0 = 0,
}

impl core::fmt::Display for DelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DelegateVersion::V0 => write!(f, "V0"),
    }
  }
}

impl TryFrom<u8> for DelegateVersion {
  type Error = UnknownDelegateVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      0 => Ok(DelegateVersion::V0),
      _ => Err(UnknownDelegateVersion(v)),
    }
  }
}

#[cfg(feature = "rkyv")]
const _: () = {
  impl From<ArchivedDelegateVersion> for DelegateVersion {
    fn from(value: ArchivedDelegateVersion) -> Self {
      match value {
        ArchivedDelegateVersion::V0 => Self::V0,
      }
    }
  }

  impl From<DelegateVersion> for ArchivedDelegateVersion {
    fn from(value: DelegateVersion) -> Self {
      match value {
        DelegateVersion::V0 => Self::V0,
      }
    }
  }
};

/// Unknown protocol version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid protocol version")]
pub struct UnknownProtocolVersion(u8);

/// Protocol version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(
    derive(Debug, Copy, Clone, Eq, PartialEq, Hash),
    repr(u8),
    non_exhaustive
  )
)]
#[non_exhaustive]
#[repr(u8)]
pub enum ProtocolVersion {
  /// Version 0
  #[default]
  V0 = 0,
}

impl core::fmt::Display for ProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::V0 => write!(f, "V0"),
    }
  }
}

impl TryFrom<u8> for ProtocolVersion {
  type Error = UnknownProtocolVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      0 => Ok(Self::V0),
      _ => Err(UnknownProtocolVersion(v)),
    }
  }
}

#[cfg(feature = "rkyv")]
const _: () = {
  impl From<ArchivedProtocolVersion> for ProtocolVersion {
    fn from(value: ArchivedProtocolVersion) -> Self {
      match value {
        ArchivedProtocolVersion::V0 => Self::V0,
      }
    }
  }

  impl From<ProtocolVersion> for ArchivedProtocolVersion {
    fn from(value: ProtocolVersion) -> Self {
      match value {
        ProtocolVersion::V0 => Self::V0,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_delegate_version() {
    assert_eq!(DelegateVersion::V0 as u8, 0);
    assert_eq!(DelegateVersion::V0.to_string(), "V0");
    assert_eq!(DelegateVersion::try_from(0), Ok(DelegateVersion::V0));
    assert_eq!(DelegateVersion::try_from(1), Err(UnknownDelegateVersion(1)));
  }

  #[test]
  fn test_protocol_version() {
    assert_eq!(ProtocolVersion::V0 as u8, 0);
    assert_eq!(ProtocolVersion::V0.to_string(), "V0");
    assert_eq!(ProtocolVersion::try_from(0), Ok(ProtocolVersion::V0));
    assert_eq!(ProtocolVersion::try_from(1), Err(UnknownProtocolVersion(1)));
  }
}
