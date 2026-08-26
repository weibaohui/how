//! Minimal logger mimicking Go's default `log` package output
//! (`2006/01/02 15:04:05 <message>` written to stderr).

use chrono::Local;
use std::fmt::Display;

/// Log a message with a Go-style timestamp prefix to stderr.
pub fn log<T: Display>(msg: T) {
    eprintln!("{} {}", Local::now().format("%Y/%m/%d %H:%M:%S"), msg);
}

/// Log with a format string and arguments (like Go's `log.Printf`).
pub fn logf<T: Display>(args: std::fmt::Arguments<'_>) {
    log(args);
}

/// Print without timestamp (matches `log.SetFlags(0)` used by the test API).
pub fn log_plain<T: Display>(msg: T) {
    eprintln!("{}", msg);
}
