use sha2::{Digest, Sha256};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_helpers_are_stable() {
        assert_eq!(
            sha256_hex(b"abcd"),
            "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
        );
    }
}
