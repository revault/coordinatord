use crate::config::{datadir_path, BitcoindConfig, Config, ConfigError};
use revault_net::noise::PublicKey as NoisePubKey;

use std::{fs, net::SocketAddr, os::unix::fs::DirBuilderExt, path::PathBuf, str::FromStr};

pub struct CoordinatorD {
    // Noise communication keys
    pub managers_keys: Vec<NoisePubKey>,
    pub stakeholders_keys: Vec<NoisePubKey>,
    pub watchtowers_keys: Vec<NoisePubKey>,

    // Misc daemon stuff
    pub data_dir: PathBuf,
    pub daemon: bool,
    pub listen: SocketAddr,

    // For storing the signatures and spend transactions
    pub postgres_config: tokio_postgres::Config,

    pub bitcoind_config: BitcoindConfig,
}

fn create_datadir(datadir_path: &PathBuf) -> Result<(), std::io::Error> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).recursive(true).create(datadir_path)
}

impl CoordinatorD {
    pub fn from_config(config: Config) -> Result<CoordinatorD, Box<dyn std::error::Error>> {
        // FIXME: upstream should use sodiumoxide's Curve 25519
        let managers_keys = config.managers.into_iter().map(|x| x.key).collect();
        let stakeholders_keys = config.stakeholders.into_iter().map(|x| x.key).collect();
        let watchtowers_keys = config.watchtowers.into_iter().map(|x| x.key).collect();

        let mut data_dir = config.data_dir.unwrap_or(datadir_path()?);
        if !data_dir.as_path().exists() {
            if let Err(e) = create_datadir(&data_dir) {
                return Err(Box::from(ConfigError(format!(
                    "Could not create data dir '{:?}': {}.",
                    data_dir,
                    e.to_string()
                ))));
            }
        }
        data_dir = fs::canonicalize(data_dir)?;
        let daemon = config.daemon.unwrap_or(false);
        let listen = config
            .listen
            // Default port is decimal representation of ₿'s unicode number
            .unwrap_or_else(|| SocketAddr::from_str("127.0.0.1:8383").unwrap());

        let postgres_config = tokio_postgres::Config::from_str(&config.postgres_uri)?;

        Ok(CoordinatorD {
            managers_keys,
            stakeholders_keys,
            watchtowers_keys,
            data_dir,
            daemon,
            listen,
            postgres_config,
            bitcoind_config: config.bitcoind_config,
        })
    }

    fn file_from_datadir(&self, file_name: &str) -> PathBuf {
        let data_dir_str = self
            .data_dir
            .to_str()
            .expect("Impossible: the datadir path is valid unicode");

        [data_dir_str, file_name].iter().collect()
    }

    pub fn pid_file(&self) -> PathBuf {
        self.file_from_datadir("revaultd.pid")
    }

    pub fn log_file(&self) -> PathBuf {
        self.file_from_datadir("log")
    }

    pub fn secret_file(&self) -> PathBuf {
        self.file_from_datadir("noise_secret")
    }
}
