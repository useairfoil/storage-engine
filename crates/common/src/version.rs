pub const VERSION: &str = resolve_version(option_env!("SE_VERSION"), env!("CARGO_PKG_VERSION"));

/// Resolves the build version from `SE_VERSION`, falling back to the package version.
const fn resolve_version<'a>(env_version: Option<&'a str>, pkg_version: &'a str) -> &'a str {
    match env_version {
        Some(version) => {
            let trimmed = version.trim_ascii();

            if trimmed.is_empty() {
                pkg_version
            } else {
                strip_leading_v(trimmed)
            }
        }
        None => pkg_version,
    }
}

/// Strips a leading `v` prefix when followed by an ASCII digit.
const fn strip_leading_v(version: &str) -> &str {
    let bytes = version.as_bytes();

    if bytes.len() >= 2 && bytes[0] == b'v' && bytes[1].is_ascii_digit() {
        version.split_at(1).1
    } else {
        version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_version_from_env_or_package() {
        assert_eq!(resolve_version(None, "0.2.0"), "0.2.0");
        assert_eq!(resolve_version(Some(""), "0.2.0"), "0.2.0");
        assert_eq!(resolve_version(Some("   "), "0.2.0"), "0.2.0");
        assert_eq!(
            resolve_version(Some("0.3.0-alpha.1"), "0.2.0"),
            "0.3.0-alpha.1"
        );
        assert_eq!(
            resolve_version(Some("v0.3.0-alpha.1"), "0.2.0"),
            "0.3.0-alpha.1"
        );
        assert_eq!(resolve_version(Some(" v1.2.3 \n"), "0.2.0"), "1.2.3");
        assert_eq!(resolve_version(Some("vnext"), "0.2.0"), "vnext");
    }
}
