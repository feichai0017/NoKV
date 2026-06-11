use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use fuser::{Errno, ReplyXattr};
use nokv_meta::XattrSetMode;

const XATTR_CREATE: i32 = 0x1;
const XATTR_REPLACE: i32 = 0x2;

pub(super) fn xattr_unsupported_error() -> Errno {
    Errno::EOPNOTSUPP
}

pub(super) fn xattr_missing_error() -> Errno {
    Errno::NO_XATTR
}

pub(super) fn xattr_name(name: &OsStr) -> Result<&[u8], Errno> {
    let name = name.as_bytes();
    if name.is_empty() || name.contains(&0) {
        return Err(Errno::EINVAL);
    }
    Ok(name)
}

pub(super) fn xattr_set_mode(flags: i32) -> Result<XattrSetMode, Errno> {
    if flags & !(XATTR_CREATE | XATTR_REPLACE) != 0 {
        return Err(Errno::EINVAL);
    }
    match (flags & XATTR_CREATE != 0, flags & XATTR_REPLACE != 0) {
        (false, false) => Ok(XattrSetMode::Any),
        (true, false) => Ok(XattrSetMode::Create),
        (false, true) => Ok(XattrSetMode::Replace),
        (true, true) => Err(Errno::EINVAL),
    }
}

pub(super) fn reply_xattr_data(data: &[u8], size: u32, reply: ReplyXattr) {
    if size == 0 {
        reply.size(u32::try_from(data.len()).unwrap_or(u32::MAX));
        return;
    }
    let requested = usize::try_from(size).unwrap_or(usize::MAX);
    if requested < data.len() {
        reply.error(Errno::ERANGE);
    } else {
        reply.data(data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xattr_errors_are_explicit_not_unimplemented() {
        assert_eq!(xattr_unsupported_error().code(), Errno::EOPNOTSUPP.code());
        assert_ne!(xattr_unsupported_error().code(), Errno::ENOSYS.code());
        assert_eq!(xattr_missing_error().code(), Errno::NO_XATTR.code());
        assert_ne!(xattr_missing_error().code(), Errno::ENOSYS.code());
        assert_eq!(
            xattr_name(OsStr::new("user.comment")).unwrap(),
            b"user.comment"
        );
        assert_eq!(
            xattr_name(OsStr::new("")).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(xattr_set_mode(0).unwrap(), XattrSetMode::Any);
        assert_eq!(xattr_set_mode(XATTR_CREATE).unwrap(), XattrSetMode::Create);
        assert_eq!(
            xattr_set_mode(XATTR_REPLACE).unwrap(),
            XattrSetMode::Replace
        );
        assert_eq!(
            xattr_set_mode(XATTR_CREATE | XATTR_REPLACE)
                .unwrap_err()
                .code(),
            Errno::EINVAL.code()
        );
    }
}
