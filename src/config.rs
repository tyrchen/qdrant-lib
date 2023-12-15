use std::env;

use config::{Config, ConfigError, Environment, File, FileFormat, Source};
use serde::Deserialize;
use storage::types::StorageConfig;
use tracing::{error, warn};
use validator::Validate;

const DEFAULT_CONFIG: &str = include_str!("../config/config.yaml");

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct Settings {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[validate]
    pub storage: StorageConfig,
    #[serde(default = "default_telemetry_disabled")]
    pub telemetry_disabled: bool,
}

impl Settings {
    #[allow(dead_code)]
    pub fn new(custom_config_path: Option<String>) -> Result<Self, ConfigError> {
        let config_exists = |path| File::with_name(path).collect().is_ok();

        // Check if custom config file exists, report error if not
        if let Some(ref path) = custom_config_path {
            if !config_exists(path) {
                error!("Config file via --config-path is not found: {path}");
            }
        }

        let env = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        let config_path_env = format!("config/{env}");

        // Report error if main or env config files exist, report warning if not
        // Check if main and env configuration file

        ["config/config", &config_path_env]
            .into_iter()
            .filter(|path| !config_exists(path))
            .for_each(|path| warn!("Config file not found: {path}"));

        // Configuration builder: define different levels of configuration files
        let mut config = Config::builder()
            // Start with compile-time base config
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml))
            // Merge main config: config/config
            .add_source(File::with_name("config/config").required(false))
            // Merge env config: config/{env}
            // Uses RUN_MODE, defaults to 'development'
            .add_source(File::with_name(&config_path_env).required(false))
            // Merge local config, not tracked in git: config/local
            .add_source(File::with_name("config/local").required(false));

        // Merge user provided config with --config-path
        if let Some(path) = custom_config_path {
            config = config.add_source(File::with_name(&path).required(false));
        }

        // Merge environment settings
        // E.g.: `QDRANT_DEBUG=1 ./target/app` would set `debug=true`
        config = config.add_source(Environment::with_prefix("QDRANT").separator("__"));

        // Build and merge config and deserialize into Settings, attach any load errors we had
        let settings: Settings = config.build()?.try_deserialize()?;
        Ok(settings)
    }
}

fn default_log_level() -> String {
    "INFO".to_string()
}

const fn default_telemetry_disabled() -> bool {
    false
}
