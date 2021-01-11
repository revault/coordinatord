mod config;
mod coordinatord;
use crate::{config::Config, coordinatord::CoordinatorD};

use std::{env, path::PathBuf, process, str::FromStr};

use daemonize_simple::Daemonize;
use tokio::{net::TcpListener, runtime::Builder as RuntimeBuilder};

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

async fn tokio_main(coordinatord: CoordinatorD) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(coordinatord.listen).await?;

    loop {
        let (_, _) = listener.accept().await?;

        tokio::spawn(async move { () });
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

    rt.block_on(tokio_main(coordinatord)).unwrap_or_else(|e| {
        log::error!("Error in event loop: {}", e);
        process::exit(1);
    });
}
