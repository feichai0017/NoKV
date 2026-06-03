use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryName {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameError {
    Empty,
    Dot,
    DotDot,
    ContainsSlash,
    ContainsNul,
}

impl DentryName {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, NameError> {
        let bytes = bytes.into();
        validate_name(&bytes)?;
        Ok(Self { bytes })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

fn validate_name(bytes: &[u8]) -> Result<(), NameError> {
    match bytes {
        [] => return Err(NameError::Empty),
        b"." => return Err(NameError::Dot),
        b".." => return Err(NameError::DotDot),
        _ => {}
    }
    if bytes.contains(&b'/') {
        return Err(NameError::ContainsSlash);
    }
    if bytes.contains(&0) {
        return Err(NameError::ContainsNul);
    }
    Ok(())
}

impl AsRef<[u8]> for DentryName {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "dentry name is empty"),
            Self::Dot => write!(f, "dentry name '.' is reserved"),
            Self::DotDot => write!(f, "dentry name '..' is reserved"),
            Self::ContainsSlash => write!(f, "dentry name contains '/'"),
            Self::ContainsNul => write!(f, "dentry name contains NUL"),
        }
    }
}

impl std::error::Error for NameError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dentry_name_rejects_reserved_names_and_path_separators() {
        assert_eq!(DentryName::new(Vec::<u8>::new()), Err(NameError::Empty));
        assert_eq!(DentryName::new(b".".to_vec()), Err(NameError::Dot));
        assert_eq!(DentryName::new(b"..".to_vec()), Err(NameError::DotDot));
        assert_eq!(
            DentryName::new(b"a/b".to_vec()),
            Err(NameError::ContainsSlash)
        );
        assert_eq!(
            DentryName::new(b"a\0b".to_vec()),
            Err(NameError::ContainsNul)
        );
    }

    #[test]
    fn dentry_name_keeps_original_bytes() {
        let name = DentryName::new(b"artifact.parquet".to_vec()).unwrap();
        assert_eq!(name.as_bytes(), b"artifact.parquet");
    }
}
