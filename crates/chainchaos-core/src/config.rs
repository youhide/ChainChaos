//! YAML fault configuration.
//!
//! ```yaml
//! faults:
//!   - type: delay
//!     method: eth_getLogs
//!     duration: 1500ms
//!     count: 3
//! ```
//!
//! This is the Phase 1 "static rules" format. The timed scenario format
//! (`seed:` + `scenario:` with `after:` triggers) arrives in Phase 2.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::fault::{Fault, FaultRule, MethodMatcher};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config{}: {source}", .path.as_ref().map(|p| format!(" in {}", p.display())).unwrap_or_default())]
    Parse {
        path: Option<PathBuf>,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("invalid fault rule faults[{index}]: {message}")]
    Invalid { index: usize, message: String },
}

/// A validated set of fault rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultConfig {
    pub rules: Vec<FaultRule>,
}

impl FaultConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text, Some(path))
    }

    pub fn from_yaml(text: &str) -> Result<Self, ConfigError> {
        Self::parse(text, None)
    }

    fn parse(text: &str, path: Option<&Path>) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            serde_yaml_ng::from_str(text).map_err(|source| ConfigError::Parse {
                path: path.map(Path::to_owned),
                source,
            })?;
        let rules = raw
            .faults
            .into_iter()
            .enumerate()
            .map(|(index, rule)| rule.validate(index))
            .collect::<Result<_, _>>()?;
        Ok(Self { rules })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    faults: Vec<RawRule>,
}

/// On-disk shape of a rule. Common fields are repeated per variant so that
/// `deny_unknown_fields` can catch typos (serde cannot combine it with
/// `flatten`).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RawRule {
    Delay {
        method: Option<String>,
        count: Option<u64>,
        #[serde(deserialize_with = "crate::duration::deserialize")]
        duration: Duration,
    },
}

impl RawRule {
    fn validate(self, index: usize) -> Result<FaultRule, ConfigError> {
        let invalid = |message: &str| ConfigError::Invalid {
            index,
            message: message.to_owned(),
        };
        let (fault, method, count) = match self {
            RawRule::Delay {
                method,
                count,
                duration,
            } => (Fault::Delay { duration }, method, count),
        };
        let method = match method {
            None => MethodMatcher::Any,
            Some(m) if m.trim().is_empty() => return Err(invalid("`method` must not be empty")),
            Some(m) => MethodMatcher::Exact(m),
        };
        if count == Some(0) {
            return Err(invalid(
                "`count` must be at least 1 (omit it for unlimited)",
            ));
        }
        Ok(FaultRule {
            fault,
            method,
            count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_delay_rule() {
        let config = FaultConfig::from_yaml(
            "faults:\n  - type: delay\n    method: eth_getLogs\n    duration: 1500ms\n    count: 3\n",
        )
        .unwrap();
        assert_eq!(
            config.rules,
            vec![FaultRule {
                fault: Fault::Delay {
                    duration: Duration::from_millis(1500)
                },
                method: MethodMatcher::Exact("eth_getLogs".into()),
                count: Some(3),
            }]
        );
    }

    #[test]
    fn empty_config_is_valid() {
        assert_eq!(FaultConfig::from_yaml("{}").unwrap().rules, vec![]);
    }

    #[test]
    fn rejects_unknown_fields_and_types() {
        let typo = FaultConfig::from_yaml("faults:\n  - type: delay\n    duraton: 1s\n");
        assert!(typo.is_err());
        let unknown = FaultConfig::from_yaml("faults:\n  - type: explode\n");
        assert!(unknown.is_err());
    }

    #[test]
    fn rejects_zero_count() {
        let err =
            FaultConfig::from_yaml("faults:\n  - type: delay\n    duration: 1s\n    count: 0\n")
                .unwrap_err();
        assert!(err.to_string().contains("faults[0]"), "{err}");
    }

    #[test]
    fn bundled_examples_are_valid() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let mut checked = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "yaml") {
                FaultConfig::from_path(&path).unwrap_or_else(|e| panic!("{e}"));
                checked += 1;
            }
        }
        assert!(checked > 0, "no example configs found in {}", dir.display());
    }
}
