use crate::config::BitcoindConfig;
use jsonrpc::{
    arg,
    client::Client,
    error::Error,
    simple_http::{Error as HttpError, SimpleHttpTransport},
};
use revault_net::bitcoin::{consensus::encode, Transaction};
use serde_json::Value as Json;
use std::any::Any;
use std::{fmt, fs};
use tokio::time::{Duration, Instant};

macro_rules! params {
    ($($param:expr),* $(,)?) => {
        [
            $(
                arg($param),
            )*
        ]
    };
}

// If bitcoind takes more than 1 minute to answer one of our queries, fail.
const RPC_SOCKET_TIMEOUT: u64 = 60;

#[derive(Debug)]
pub enum BitcoindError {
    /// It can be related to us..
    CookieFile(std::io::Error),
    /// Or directly to bitcoind's RPC server
    Server(Error),
    BatchMissingResponse,
}

impl fmt::Display for BitcoindError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::CookieFile(e) => write!(f, "While reading the cookie file: {}", e),
            Self::Server(e) => write!(f, "RPC server error: {}", e),
            Self::BatchMissingResponse => write!(f, "Batch is missing a response"),
        }
    }
}

impl std::error::Error for BitcoindError {}

impl From<HttpError> for BitcoindError {
    fn from(e: HttpError) -> Self {
        Self::Server(Error::Transport(Box::new(e)))
    }
}

pub struct BitcoinD {
    node_client: Client,
    pub broadcast_interval: Duration,
}

impl BitcoinD {
    pub async fn new(config: &BitcoindConfig) -> Result<BitcoinD, BitcoindError> {
        let cookie_string =
            fs::read_to_string(&config.cookie_path).map_err(|e| BitcoindError::CookieFile(e))?;

        let node_client = Client::with_transport(
            SimpleHttpTransport::builder()
                .url(&config.addr.to_string())
                .map_err(BitcoindError::from)?
                .timeout(Duration::from_secs(RPC_SOCKET_TIMEOUT))
                .cookie_auth(cookie_string)
                .build(),
        );

        Ok(BitcoinD {
            node_client,
            broadcast_interval: config.broadcast_interval,
        })
    }

    // Reasonably try to be robust to possible spurious communication error.
    fn handle_error(&self, e: jsonrpc::Error, start: Instant) -> Result<(), BitcoindError> {
        let now = Instant::now();

        match e {
            jsonrpc::Error::Transport(ref err) => {
                log::error!("Transport error when talking to bitcoind: '{}'", err);

                // This is *always* a HttpError. Rule out the error that can
                // not occur after startup (ie if we encounter them it must be startup
                // and we better be failing quickly).
                let any_err = err as &dyn Any;
                if let Some(http_err) = any_err.downcast_ref::<HttpError>() {
                    match http_err {
                        HttpError::InvalidUrl { .. } => return Err(BitcoindError::Server(e)),
                        // FIXME: allow it to be unreachable for a handful of seconds,
                        // but not at startup!
                        HttpError::SocketError(_) => return Err(BitcoindError::Server(e)),
                        HttpError::HttpParseError => {
                            // Weird. Try again once, just in case.
                            if now.duration_since(start) > Duration::from_secs(1) {
                                return Err(BitcoindError::Server(e));
                            }
                            std::thread::sleep(Duration::from_secs(1));
                        }
                        _ => {}
                    }
                }

                // This one *may* happen. For a number of reasons, the obvious one may
                // be the RPC work queue being exceeded. In this case, and since we'll
                // usually fail if we err try again for a reasonable amount of time.
                if now.duration_since(start) > Duration::from_secs(45) {
                    return Err(BitcoindError::Server(e));
                }
                std::thread::sleep(Duration::from_secs(1));
                log::debug!("Retrying RPC request to bitcoind.");
            }
            jsonrpc::Error::Json(ref err) => {
                // Weird. A JSON serialization error? Just try again but
                // fail fast anyways as it should not happen.
                log::error!(
                    "JSON serialization error when talking to bitcoind: '{}'",
                    err
                );
                if now.duration_since(start) > Duration::from_secs(1) {
                    return Err(BitcoindError::Server(e));
                }
                std::thread::sleep(Duration::from_millis(500));
                log::debug!("Retrying RPC request to bitcoind.");
            }
            _ => return Err(BitcoindError::Server(e)),
        };

        Ok(())
    }

    fn make_requests(
        &self,
        reqs: &[jsonrpc::Request],
    ) -> Result<Vec<Result<Json, BitcoindError>>, BitcoindError> {
        log::debug!("Sending to bitcoind: {:#?}", reqs);

        // Trying to be robust on bitcoind's spurious failures. We try to support bitcoind failing
        // under our feet for a few dozens of seconds, while not delaying an early failure (for
        // example, if we got the RPC listening address or path to the cookie wrong).
        let start = Instant::now();
        loop {
            match self.node_client.send_batch(reqs) {
                Ok(resp) => {
                    log::debug!("Got from bitcoind: {:#?}", resp);
                    let res = resp
                        .into_iter()
                        .flatten()
                        .map(|resp| resp.result().map_err(BitcoindError::Server))
                        .collect::<Vec<Result<Json, BitcoindError>>>();

                    // FIXME: why is rust-jsonrpc even returning a Vec of Option in the first
                    // place??
                    if res.len() != reqs.len() {
                        return Err(BitcoindError::BatchMissingResponse);
                    }

                    return Ok(res);
                }
                Err(e) => {
                    // Decide wether we should error, or not yet
                    self.handle_error(e, start)?;
                }
            }
        }
    }

    /// Broadcast a batch of transactions with 'sendrawtransaction'
    pub fn broadcast_transactions(
        &self,
        txs: &[Transaction],
    ) -> Result<Vec<Result<Json, BitcoindError>>, BitcoindError> {
        let txs_hex: Vec<[Box<serde_json::value::RawValue>; 1]> = txs
            .iter()
            .map(|tx| params!(Json::String(encode::serialize_hex(tx))))
            .collect();
        log::debug!("Batch-broadcasting {:?}", txs_hex);
        let reqs: Vec<jsonrpc::Request> = txs_hex
            .iter()
            .map(|hex| {
                self.node_client
                    .build_request("sendrawtransaction", hex.as_ref())
            })
            .collect();
        self.make_requests(&reqs)
    }
}
