//! The bridge of the examples of scsynth-client. Its README says how to run
//! it.

use std::env;
use std::process::ExitCode;

use scsynth_client_bridge::{Action, USAGE, parse_args, serve};
use tokio::net::TcpListener;
use tokio::runtime::Builder;

fn main() -> ExitCode {
    let config = match parse_args(env::args().skip(1)) {
        Ok(Action::Run(config)) => config,
        Ok(Action::Help) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("bridge: {error}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("bridge: cannot start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async {
        match TcpListener::bind(config.listen).await {
            Ok(listener) => {
                serve(listener, config).await;
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("bridge: cannot listen on {}: {error}", config.listen);
                ExitCode::FAILURE
            }
        }
    })
}
