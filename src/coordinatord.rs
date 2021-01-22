use crate::config::{datadir_path, Config, ConfigError};
use revault_net::{
    bitcoin::{secp256k1::PublicKey, OutPoint, Txid},
    message::server::RevaultSignature,
    noise::NoisePubKey,
};

use std::{
    collections::HashMap, fs, net::SocketAddr, os::unix::fs::DirBuilderExt, path::PathBuf,
    str::FromStr,
};

pub type SigCache = HashMap<Txid, HashMap<PublicKey, RevaultSignature>>;
pub type SpendTxCache = HashMap<OutPoint, Vec<u8>>;

pub struct CoordinatorD {
    // Noise communication keys
    pub managers_keys: Vec<NoisePubKey>,
    pub stakeholders_keys: Vec<NoisePubKey>,
    pub watchtowers_keys: Vec<NoisePubKey>,

    // Misc daemon stuff
    pub data_dir: PathBuf,
    pub daemon: bool,
    pub listen: SocketAddr,

    // Cache. FIXME: implement permanent storage
    pub postgres_config: tokio_postgres::Config,
    pub stk_sigs: SigCache,
    pub spend_txs: SpendTxCache,
}

pub fn noise_keys_from_strlist(
    keys_str: Vec<String>,
) -> Result<Vec<NoisePubKey>, revault_net::Error> {
    keys_str
        .into_iter()
        .map(|s| NoisePubKey::from_str(&s))
        .collect()
}

fn create_datadir(datadir_path: &PathBuf) -> Result<(), std::io::Error> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).recursive(true).create(datadir_path)
}

impl CoordinatorD {
    pub fn from_config(config: Config) -> Result<CoordinatorD, Box<dyn std::error::Error>> {
        let managers_keys = noise_keys_from_strlist(config.managers)?;
        let stakeholders_keys = noise_keys_from_strlist(config.stakeholders)?;
        let watchtowers_keys = noise_keys_from_strlist(config.watchtowers)?;

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

        let stk_sigs = HashMap::new();
        let spend_txs = HashMap::new();

        let postgres_config = tokio_postgres::Config::from_str(&config.postgres_uri)?;

        Ok(CoordinatorD {
            managers_keys,
            stakeholders_keys,
            watchtowers_keys,
            data_dir,
            daemon,
            listen,
            postgres_config,
            stk_sigs,
            spend_txs,
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
