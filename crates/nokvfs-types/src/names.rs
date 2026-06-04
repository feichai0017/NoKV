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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathError {
    Empty,
    Relative,
    ParentTraversal,
    InvalidName(NameError),
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

pub fn parse_absolute_path(path: &str) -> Result<Vec<DentryName>, PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    if !path.starts_with('/') {
        return Err(PathError::Relative);
    }
    let mut out = Vec::new();
    for raw in path.split('/').filter(|part| !part.is_empty()) {
        if raw == "." {
            continue;
        }
        if raw == ".." {
            return Err(PathError::ParentTraversal);
        }
        out.push(DentryName::new(raw.as_bytes().to_vec()).map_err(PathError::InvalidName)?);
    }
    Ok(out)
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

impl fmt::Display for PathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "path is empty"),
            Self::Relative => write!(f, "path is not absolute"),
            Self::ParentTraversal => write!(f, "path parent traversal is not allowed"),
            Self::InvalidName(err) => write!(f, "invalid path component: {err}"),
        }
    }
}

impl std::error::Error for PathError {}

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

    #[test]
    fn parse_absolute_path_normalizes_slashes_and_dot() {
        let components = parse_absolute_path("/workspace/./artifacts//model.bin").unwrap();
        let names: Vec<Vec<u8>> = components.into_iter().map(DentryName::into_bytes).collect();
        assert_eq!(
            names,
            vec![
                b"workspace".to_vec(),
                b"artifacts".to_vec(),
                b"model.bin".to_vec()
            ]
        );
    }

    #[test]
    fn parse_absolute_path_rejects_relative_and_parent_traversal() {
        assert_eq!(parse_absolute_path("workspace"), Err(PathError::Relative));
        assert_eq!(
            parse_absolute_path("/workspace/../secret"),
            Err(PathError::ParentTraversal)
        );
    }
}
