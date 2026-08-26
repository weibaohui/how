//! Shared command-line argument helpers for the WSP binaries.
//!
//! These mimic Go's `flag` package: a flag is given as `-name value`,
//! `-name=value`, `--name value` or `--name=value`. Unlike clap this accepts
//! the single-dash long form the original Go binaries use.

/// Parse a single string flag out of `std::env::args`, returning its value or
/// `default` when absent.
pub fn string_flag(name: &str, default: &str) -> String {
    let mut args = std::env::args().skip(1).peekable();
    let dash_long = format!("-{name}");
    let dashdash_long = format!("--{name}");
    let eq = format!("-{name}=");
    let ddeq = format!("--{name}=");
    while let Some(a) = args.next() {
        if a == dash_long || a == dashdash_long {
            if let Some(v) = args.next() {
                return v;
            }
        } else if let Some(rest) = a.strip_prefix(&eq) {
            return rest.to_string();
        } else if let Some(rest) = a.strip_prefix(&ddeq) {
            return rest.to_string();
        }
    }
    default.to_string()
}
