mod config;
mod coordinatord;
use crate::{config::Config, coordinatord::CoordinatorD};
use revault_net::noise::{NoisePrivKey, NoisePubKey};

use std::{env, fs, io::Read, net::TcpListener, path::PathBuf, process, str::FromStr};

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

enum MessageSender {
    Manager,
    StakeHolder,
    WatchTower,
}

async fn tokio_main(
    coordinatord: CoordinatorD,
    noise_secret: NoisePrivKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_pubkeys: Vec<NoisePubKey> = coordinatord
        .managers_keys
        .clone()
        .into_iter()
        .chain(coordinatord.stakeholders_keys.clone().into_iter())
        .chain(coordinatord.watchtowers_keys.clone().into_iter())
        .collect();
    let managers_keys = coordinatord.managers_keys;
    let stakeholders_keys = coordinatord.stakeholders_keys;
    let watchtowers_keys = coordinatord.watchtowers_keys;
    // FIXME: implement a tokio feature upstream and use Tokio's TcpListener
    let listener = TcpListener::bind(coordinatord.listen)?;

    loop {
        // This does the Noise KK handshake..
        let kk_stream =
            revault_net::transport::KKTransport::accept(&listener, &noise_secret, &client_pubkeys);

        match kk_stream {
            // .. So from here we are automagically using an AEAD stream
            Ok(mut stream) => {
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

                tokio::spawn(async move {
                    // Now, process all messages from this connection.
                    loop {
                        let msg = stream.read();
                        match msg {
                            Ok(msg) => {
                                // read() is nice: on non-fatal error (basically connection
                                // interruption) it'll just signal it by returning an empty
                                // buffer.
                                if msg.is_empty() {
                                    break;
                                }

                                match msg_sender {
                                    MessageSender::Manager => unimplemented!(),
                                    MessageSender::StakeHolder => unimplemented!(),
                                    MessageSender::WatchTower => unimplemented!(),
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "Reading error from '{:x?}': '{}'",
                                    stream.remote_static(),
                                    e
                                );
                                break;
                            }
                        }
                    }
                });
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
    let mut noise_secret_fd = fs::File::open(coordinatord.secret_file()).unwrap_or_else(|e| {
        eprintln!("Error opening Noise static private key file: '{}'", e);
        process::exit(1);
    });
    let mut noise_secret = NoisePrivKey([0; 32]);
    noise_secret_fd
        .read_exact(&mut noise_secret.0)
        .unwrap_or_else(|e| {
            eprintln!("Error reading Noise static private key file: '{}'", e);
            process::exit(1);
        });
    assert!(noise_secret.0 != [0; 32]);

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

    println!("Started revault_coordinatord");
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
