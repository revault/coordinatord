use revault_net::{noise::PublicKey as NoisePubKey, sodiumoxide};

use std::{net::SocketAddr, path::PathBuf, vec::Vec};

use serde::{de, Deserialize};

#[derive(Debug, Clone)]
pub struct NoisePubkeyHex {
    pub key: NoisePubKey,
}

impl<'de> de::Visitor<'de> for NoisePubkeyHex {
    type Value = NoisePubkeyHex;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a hex encoded string")
    }

    fn visit_str<E>(self, data: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let data = sodiumoxide::hex::decode(data).map_err(|e| {
            de::Error::custom(format!("Invalid hex in Noise public key: '{:?}'", e))
        })?;
        let key = NoisePubKey::from_slice(&data)
            .ok_or_else(|| de::Error::custom("Invalid Noise public key"))?;
        Ok(NoisePubkeyHex { key })
    }

    fn visit_borrowed_str<E>(self, data: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let data = sodiumoxide::hex::decode(data).map_err(|e| {
            de::Error::custom(format!("Invalid hex in Noise public key: '{:?}'", e))
        })?;
        let key = NoisePubKey::from_slice(&data)
            .ok_or_else(|| de::Error::custom("Invalid Noise public key"))?;
        Ok(NoisePubkeyHex { key })
    }
}

impl<'de> Deserialize<'de> for NoisePubkeyHex {
    fn deserialize<D>(deserializer: D) -> Result<NoisePubkeyHex, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let pk = NoisePubkeyHex {
            key: NoisePubKey([0u8; 32]),
        };
        deserializer.deserialize_str(pk)
    }
}

/// Static informations we require to operate
#[derive(Debug, Deserialize)]
pub struct Config {
    /// The managers Noise static public keys
    pub managers: Vec<NoisePubkeyHex>,
    /// The stakeholders Noise static public keys
    pub stakeholders: Vec<NoisePubkeyHex>,
    /// The watchtowers Noise static public keys
    pub watchtowers: Vec<NoisePubkeyHex>,
    /// PostgreSQL database connection URI, as specified in
    /// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    pub postgres_uri: String,
    /// An optional custom data directory
    pub data_dir: Option<PathBuf>,
    /// Whether to daemonize the process
    pub daemon: Option<bool>,
    /// What messages to log
    pub log_level: Option<String>,
    /// <ip:port> to bind to
    pub listen: Option<SocketAddr>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Configuration error: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Get the absolute path to our default data directory, `~/.revault_coordinatord/<network>/`
pub fn datadir_path() -> Result<PathBuf, ConfigError> {
    dirs::home_dir()
        .map(|mut path| {
            path.push(".revault_coordinatord");
            path
        })
        .ok_or_else(|| ConfigError("Could not locate the configuration directory.".to_string()))
}

fn config_file_path() -> Result<PathBuf, ConfigError> {
    datadir_path().map(|mut path| {
        path.push("config.toml");
        path
    })
}

impl Config {
    /// Get our static configuration out of a mandatory configuration file.
    ///
    /// We require all settings to be set in the configuration file, and only in the configuration
    /// file. We don't allow to set them via the command line or environment variables to avoid a
    /// futile duplication.
    pub fn from_file(custom_path: Option<PathBuf>) -> Result<Config, ConfigError> {
        let config_file = custom_path.unwrap_or(config_file_path()?);

        let config = std::fs::read(&config_file)
            .map_err(|e| ConfigError(format!("Reading configuration file: {}", e)))
            .and_then(|file_content| {
                toml::from_slice::<Config>(&file_content)
                    .map_err(|e| ConfigError(format!("Parsing configuration file: {}", e)))
            })?;

        let stk_len = config.stakeholders.len();
        let wt_len = config.watchtowers.len();
        if stk_len > wt_len {
            return Err(ConfigError(format!(
                "Not enough watchtowers ({} stakeholders, but only {} watchtowers)",
                stk_len, wt_len
            )));
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::{config_file_path, Config};

    // Test the format of the configuration file
    #[test]
    fn deserialize_toml_config() {
        // A valid config
        let toml_str = r#"
            daemon = true
            data_dir = "/home/wizardsardine/custom/folder/"
            listen = "127.0.0.1:11111"
            postgres_uri = "postgresql://user:secret@localhost"

            managers = ["61feafb2db96bf650b496c74c24ce92fa608e271b4092405f3364c9f8466df66", "b7f56d2b69ea6d8ae0c6e0fb6a7b85a03493bb0771e1b06a69f5f45c017512a9"]
            stakeholders = ["cf14ea57f99801da8ee71b7d8e63255ef0fe685e87dd57d8a4a2603a29805ba2", "e74a2cfabd850fea3668fd6fc00d89849faf9537deadb68648a7f9e4217b0a0c",
                            "14f5cd87c7f09e1e7542ca4fc874bd113cfa47c68c8927fcf8f2c07819fd86da", "6a3f052859e7eae3574b657fe3710c698f6301acdda8724e6ff0f6bfa488024d"]
            watchtowers = ["17e884097e6f0fc7598dfce7bc3bcabe38107a5c186ebb0bbc80f029a2dd7ca4", "66a85b365912da419675fd11388c90c2ec9b723f42e765f7ff0dae6735dccb1a",
                            "39f246fa212256a506b7c5777910c41af2a0544b5e7d4683bde54e8ad523e850", "79ed4f33d77b57189e30caf49edb0594aa687f7ce1ab655758ddfbb5d13c95e4"]
        "#;
        let _config: Config = toml::from_str(toml_str).expect("Deserializing toml_str");
    }

    #[test]
    fn config_directory() {
        let filepath = config_file_path().expect("Getting config file path");

        #[cfg(target_os = "linux")]
        {
            assert!(filepath.as_path().starts_with("/home/"));
            assert!(filepath
                .as_path()
                .ends_with(".revault_coordinatord/config.toml"));
        }
    }
}
