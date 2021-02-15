mod config;
mod coordinatord;
mod db;
mod processing;
use crate::{
    config::Config,
    coordinatord::CoordinatorD,
    db::maybe_create_db,
    processing::{
        process_manager_message, process_stakeholder_message, process_watchtower_message,
    },
};
use revault_net::{
    bitcoin::hashes::hex::ToHex,
    noise::{PublicKey as NoisePubKey, SecretKey as NoisePrivKey},
    sodiumoxide::{self, crypto::scalarmult::curve25519},
    transport::KKTransport,
};

use std::{
    env, fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    process,
    str::FromStr,
    sync::Arc,
};

use daemonize_simple::Daemonize;
use tokio::runtime::Builder as RuntimeBuilder;

// No need for complex argument parsing: we only ever accept one, "--conf".
fn parse_args(args: Vec<String>) -> Option<PathBuf> {
    if args.len() == 1 {
        return None;
    }

    if args.len() != 3 {
        eprintln!("Unknown arguments '{:?}'.", args);
        eprintln!("Only '--conf <configuration file path>' is supported.");
        process::exit(1);
    }

    Some(PathBuf::from(args[2].to_owned()))
}

// This creates the log file automagically if it doesn't exist, and logs on stdout
// if None is given
fn setup_logger(
    log_file: Option<&str>,
    log_level: log::LevelFilter,
) -> Result<(), fern::InitError> {
    let dispatcher = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log_level);

    if let Some(log_file) = log_file {
        dispatcher.chain(fern::log_file(log_file)?).apply()?;
    } else {
        dispatcher.chain(std::io::stdout()).apply()?;
    }

    Ok(())
}

// The communication keys are (for now) hot, so we just create it ourselves on first run.
fn read_or_create_noise_key(secret_file: PathBuf) -> NoisePrivKey {
    let mut noise_secret = NoisePrivKey([0; 32]);

    if !secret_file.as_path().exists() {
        log::info!(
            "No Noise private key at '{:?}', generating a new one",
            secret_file
        );
        noise_secret = sodiumoxide::crypto::box_::gen_keypair().1;

        let mut options = fs::OpenOptions::new();
        // We create it in read-only but open it in write only.
        options.write(true).create_new(true).mode(0o400);
        let mut fd = options.open(secret_file.clone()).unwrap_or_else(|e| {
            eprintln!("Opening Noise private key file: {}", e);
            process::exit(1);
        });
        fd.write_all(&noise_secret.0).unwrap_or_else(|e| {
            eprintln!("Writing Noise private key to '{:?}': '{}'", secret_file, e);
            process::exit(1);
        });
    } else {
        let mut noise_secret_fd = fs::File::open(secret_file).unwrap_or_else(|e| {
            eprintln!("Error opening Noise static private key file: '{}'", e);
            process::exit(1);
        });
        noise_secret_fd
            .read_exact(&mut noise_secret.0)
            .unwrap_or_else(|e| {
                eprintln!("Error reading Noise static private key file: '{}'", e);
                process::exit(1);
            });
    }

    // TODO: have a decent memory management and mlock() the key

    assert!(noise_secret.0 != [0; 32]);
    noise_secret
}

#[derive(Debug)]
enum MessageSender {
    Manager,
    StakeHolder,
    WatchTower,
}

// Process all messages from this connection
async fn connection_handler(
    mut stream: KKTransport,
    msg_sender: MessageSender,
    pg_config: Arc<tokio_postgres::Config>,
) {
    loop {
        match stream.read() {
            Ok(msg) => {
                // read() is nice: on non-fatal error (basically connection
                // interruption) it'll just signal it by returning an empty
                // buffer.
                if msg.is_empty() {
                    log::trace!("Empty message, connection was ended by peer.");
                    return;
                }
                log::trace!(
                    "Got message '{}' (raw: '{:x?}') from {:?}",
                    String::from_utf8_lossy(&msg),
                    msg,
                    msg_sender
                );

                let response = match msg_sender {
                    MessageSender::Manager => process_manager_message(&*pg_config, msg).await,
                    MessageSender::StakeHolder => {
                        process_stakeholder_message(&*pg_config, msg).await
                    }
                    MessageSender::WatchTower => process_watchtower_message(&*pg_config, msg).await,
                };

                // We close the connection on processing or response-writing
                // error.
                match response {
                    Ok(Some(response)) => {
                        log::trace!("Responding with {:x?}", response);
                        if let Err(e) = stream.write(&response) {
                            log::error!(
                                "Writing response '{:x?}' to '{:x?}': '{}'",
                                response,
                                stream.remote_static(),
                                e
                            );
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        log::error!(
                            "Processing message from '{:x?}': '{}'",
                            stream.remote_static(),
                            e
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "Reading error from '{:x?}': '{}'",
                    stream.remote_static(),
                    e
                );
                return;
            }
        }
    }
}

async fn tokio_main(
    coordinatord: CoordinatorD,
    noise_secret: NoisePrivKey,
) -> Result<(), Box<dyn std::error::Error>> {
    // We use PostgreSQL for storing the signatures and spend transactions. That may
    // seem overkill for now, but this server is expected to grow and we'll probably
    // use more Postgre feature soon. For one, Postgre makes it easy to setup database
    // replication.
    maybe_create_db(&coordinatord.postgres_config).await?;
    let postgres_config = Arc::new(coordinatord.postgres_config);

    // Who we are accepting connections from. Note that we of course trust them and
    // therefore don't make a big deal of DOS protection.
    let managers_keys = coordinatord.managers_keys;
    let stakeholders_keys = coordinatord.stakeholders_keys;
    let watchtowers_keys = coordinatord.watchtowers_keys;
    let client_pubkeys: Vec<NoisePubKey> = managers_keys
        .clone()
        .into_iter()
        .chain(stakeholders_keys.clone().into_iter())
        .chain(watchtowers_keys.clone().into_iter())
        .collect();

    // FIXME: implement a tokio feature upstream and use Tokio's TcpListener
    let listener = TcpListener::bind(coordinatord.listen)?;

    loop {
        // This does the Noise KK handshake..
        let kk_stream =
            revault_net::transport::KKTransport::accept(&listener, &noise_secret, &client_pubkeys);

        match kk_stream {
            // .. So from here we are automagically using an AEAD stream
            Ok(stream) => {
                // Now figure out who's talking to us
                let their_pubkey = stream.remote_static();
                let msg_sender = if managers_keys.contains(&their_pubkey) {
                    MessageSender::Manager
                } else if stakeholders_keys.contains(&their_pubkey) {
                    MessageSender::StakeHolder
                } else if watchtowers_keys.contains(&their_pubkey) {
                    MessageSender::WatchTower
                } else {
                    unreachable!("An unknown key was able to perform the handshake?")
                };
                let pg_config = postgres_config.clone();
                log::trace!(
                    "Got a new connection from a {:?} with key {:x?}",
                    msg_sender,
                    their_pubkey
                );

                tokio::spawn(
                    async move { connection_handler(stream, msg_sender, pg_config).await },
                );
            }
            Err(e) => {
                log::error!("Accepting new connection: '{}'", e);
            }
        }
    }
}

fn main() {
    #[cfg(not(target_os = "linux"))]
    {
        // FIXME: All Unix should be fine?
        eprintln!("Only Linux is supported for now.");
        process::exit(1);
    }

    let args = env::args().collect();
    let conf_file = parse_args(args);
    let config = Config::from_file(conf_file).unwrap_or_else(|e| {
        eprintln!("Error parsing config: {}", e);
        process::exit(1);
    });
    let log_level = if let Some(ref level) = &config.log_level {
        log::LevelFilter::from_str(level.as_str()).unwrap_or_else(|e| {
            eprintln!("Invalid log level: {}", e);
            process::exit(1);
        })
    } else {
        log::LevelFilter::Info
    };
    let coordinatord = CoordinatorD::from_config(config).unwrap_or_else(|e| {
        eprintln!("Error creating global state: {}", e);
        process::exit(1);
    });

    sodiumoxide::init().unwrap_or_else(|_| {
        eprintln!("Error initializing libsodium.");
        process::exit(1);
    });

    let log_file = coordinatord.log_file();
    let log_output = if coordinatord.daemon {
        Some(log_file.to_str().expect("Valid unicode"))
    } else {
        None
    };
    setup_logger(log_output, log_level).unwrap_or_else(|e| {
        eprintln!("Error setting up logger: {}", e);
        process::exit(1);
    });

    // Our static noise private key. It needs to be hot, as we use it to decrypt every
    // incoming message.
    let noise_secret = read_or_create_noise_key(coordinatord.secret_file());

    // We use tokio for async processing and io (which we don't even fully implement
    // yet.. But hey that'd be a nice FIXME as a first contribution for upstream :))
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .thread_name("revault_coordinatord_worker")
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Creating tokio runtime: {}", e);
            process::exit(1);
        });

    println!(
        "Started revault_coordinatord with Noise pubkey: {}",
        NoisePubKey(curve25519::scalarmult_base(&curve25519::Scalar(noise_secret.0)).0)
            .0
            .to_hex()
    );
    log::debug!("Stakeholders keys:");
    for k in coordinatord.stakeholders_keys.iter() {
        log::debug!("   {}", k.0.to_hex());
    }
    log::debug!("Managers keys:");
    for k in coordinatord.managers_keys.iter() {
        log::debug!("   {}", k.0.to_hex());
    }
    log::debug!("Watchtowers keys:");
    for k in coordinatord.watchtowers_keys.iter() {
        log::debug!("   {}", k.0.to_hex());
    }

    if coordinatord.daemon {
        let daemon = Daemonize {
            // TODO: Make this configurable for inits
            pid_file: Some(coordinatord.pid_file()),
            ..Daemonize::default()
        };
        daemon.doit().unwrap_or_else(|e| {
            eprintln!("Error daemonizing: {}", e);
            process::exit(1);
        });
    }

    rt.block_on(tokio_main(coordinatord, noise_secret))
        .unwrap_or_else(|e| {
            log::error!("Error in event loop: {}", e);
            process::exit(1);
        });
}
