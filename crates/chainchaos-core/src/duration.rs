//! Human-friendly durations such as `250ms`, `2s` or `1m`.
//!
//! Kept deliberately tiny instead of pulling in a dependency: chainchaos only
//! needs whole numbers with a unit suffix.

use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// Parses `<integer><unit>` where unit is one of `ms`, `s`, `m`.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    let split = input
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("duration `{input}` is missing a unit (use ms, s or m)"))?;
    let (digits, unit) = input.split_at(split);
    if digits.is_empty() {
        return Err(format!("duration `{input}` must start with a number"));
    }
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("duration `{input}` is out of range"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration `{input}` is out of range")),
        other => Err(format!(
            "duration `{input}` has unknown unit `{other}` (use ms, s or m)"
        )),
    }
}

/// Serde helper for fields written as duration strings.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_duration(&raw).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(parse_duration("1500ms"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("2s"), Ok(Duration::from_secs(2)));
        assert_eq!(parse_duration(" 3m "), Ok(Duration::from_secs(180)));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("ms").is_err());
        assert!(parse_duration("1.5s").is_err());
        assert!(parse_duration("5h").is_err());
        assert!(parse_duration("-1s").is_err());
    }
}
