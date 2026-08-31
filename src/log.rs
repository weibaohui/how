//! Minimal logger mimicking Go's default `log` package output
//! (`2006/01/02 15:04:05 <message>` written to stderr), with a global
//! output level controlled by the `loglevel` config key.
//!
//! Levels (most to least severe): `error` < `warn` < `info` < `debug`.
//! The threshold is inclusive: level `info` prints error + warn + info and
//! suppresses debug. The default is `info` — the historical behaviour, where
//! every message was printed. `log_plain` (used by the test API) is
//! unconditional and not affected by the level.

use chrono::Local;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Log severity level. Ordered by severity: a message is printed when its
/// level is <= the configured threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

/// The default level when `loglevel` is absent from the config: `info`,
/// which preserves the historical "print everything" behaviour except for
/// the messages explicitly marked debug.
pub const DEFAULT_LEVEL: Level = Level::Info;

static LEVEL: AtomicUsize = AtomicUsize::new(DEFAULT_LEVEL as usize);

impl Level {
    /// Parse a level name (case-insensitive): "error", "warn" (or
    /// "warning"), "info", "debug". Anything else is an error.
    pub fn parse(s: &str) -> Result<Level, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Level::Error),
            "warn" | "warning" => Ok(Level::Warn),
            "info" => Ok(Level::Info),
            "debug" => Ok(Level::Debug),
            other => Err(format!(
                "invalid loglevel '{other}' (expected error|warn|info|debug)"
            )),
        }
    }

    /// The lower-case name used in config files and log messages.
    pub fn name(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
        }
    }
}

/// Set the global output level.
pub fn set_level(level: Level) {
    LEVEL.store(level as usize, Ordering::Relaxed);
}

/// Set the global output level from a config string. An empty string means
/// "unset" and applies the default. An invalid value logs a warning and
/// falls back to the default (so a typo cannot silently mute the process).
pub fn set_level_from_str(s: &str) {
    let level = if s.trim().is_empty() {
        DEFAULT_LEVEL
    } else {
        match Level::parse(s) {
            Ok(l) => l,
            Err(e) => {
                log_warn(format!("{e}; falling back to '{}'", DEFAULT_LEVEL.name()));
                DEFAULT_LEVEL
            }
        }
    };
    set_level(level);
}

/// The currently configured output level.
pub fn level() -> Level {
    match LEVEL.load(Ordering::Relaxed) {
        0 => Level::Error,
        1 => Level::Warn,
        3 => Level::Debug,
        _ => Level::Info,
    }
}

/// Whether a message at `level` would be printed under the current
/// threshold.
pub fn enabled(level: Level) -> bool {
    level as usize <= LEVEL.load(Ordering::Relaxed)
}

/// Emit one Go-style timestamped line to stderr.
fn emit<T: Display>(msg: T) {
    eprintln!("{} {}", Local::now().format("%Y/%m/%d %H:%M:%S"), msg);
}

/// Log at ERROR level: failures that break a request or a component
/// ("Unable to ...", accept/connection errors, panics).
pub fn log_error<T: Display>(msg: T) {
    if enabled(Level::Error) {
        emit(msg);
    }
}

/// Log at WARN level: abnormal but handled conditions — config fallbacks,
/// reaped/wedged/dead tunnels, dial backoff, timeouts.
pub fn log_warn<T: Display>(msg: T) {
    if enabled(Level::Warn) {
        emit(msg);
    }
}

/// Log at INFO level: startup and lifecycle lines, access log, periodic
/// health summaries.
pub fn log_info<T: Display>(msg: T) {
    if enabled(Level::Info) {
        emit(msg);
    }
}

/// Log at DEBUG level: per-request internals and state-change detail that
/// is chatty under load.
pub fn log_debug<T: Display>(msg: T) {
    if enabled(Level::Debug) {
        emit(msg);
    }
}

/// Log a message with a Go-style timestamp prefix to stderr.
///
/// Historical entry point kept for compatibility; equivalent to
/// [`log_info`]. Prefer the explicit `log_error` / `log_warn` / `log_info` /
/// `log_debug` in new code.
pub fn log<T: Display>(msg: T) {
    log_info(msg);
}

/// Log with a format string and arguments (like Go's `log.Printf`).
pub fn logf<T: Display>(args: std::fmt::Arguments<'_>) {
    log_info(args);
}

/// Print without timestamp (matches `log.SetFlags(0)` used by the test API).
/// Unconditional: this is program output, not a log line.
pub fn log_plain<T: Display>(msg: T) {
    eprintln!("{}", msg);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that mutate the process-global level must not run in parallel
    /// with each other (Rust runs tests on multiple threads).
    static LEVEL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn parse_accepts_known_names_case_insensitively() {
        assert_eq!(Level::parse("error").unwrap(), Level::Error);
        assert_eq!(Level::parse("WARN").unwrap(), Level::Warn);
        assert_eq!(Level::parse("Warning").unwrap(), Level::Warn);
        assert_eq!(Level::parse(" info ").unwrap(), Level::Info);
        assert_eq!(Level::parse("DEBUG").unwrap(), Level::Debug);
    }

    #[test]
    fn parse_rejects_unknown_names() {
        assert!(Level::parse("trace").is_err());
        assert!(Level::parse("verbose").is_err());
        assert!(Level::parse("").is_err());
    }

    #[test]
    fn threshold_is_inclusive_and_ordered() {
        let _g = LEVEL_LOCK.lock().unwrap();
        set_level(Level::Warn);
        assert!(enabled(Level::Error));
        assert!(enabled(Level::Warn));
        assert!(!enabled(Level::Info));
        assert!(!enabled(Level::Debug));
        assert_eq!(level(), Level::Warn);

        set_level(Level::Debug);
        assert!(enabled(Level::Error));
        assert!(enabled(Level::Info));
        assert!(enabled(Level::Debug));

        set_level(Level::Error);
        assert!(enabled(Level::Error));
        assert!(!enabled(Level::Warn));

        // Restore the default for other tests sharing this process.
        set_level(DEFAULT_LEVEL);
        assert_eq!(level(), Level::Info);
    }

    #[test]
    fn set_level_from_str_handles_empty_invalid_and_valid() {
        let _g = LEVEL_LOCK.lock().unwrap();
        set_level_from_str("");
        assert_eq!(level(), DEFAULT_LEVEL);

        set_level_from_str("nonsense");
        assert_eq!(level(), DEFAULT_LEVEL); // invalid -> default, not mute

        set_level_from_str("error");
        assert_eq!(level(), Level::Error);

        set_level(DEFAULT_LEVEL);
    }
}
