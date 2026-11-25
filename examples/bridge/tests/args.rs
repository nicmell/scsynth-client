//! The arguments of the bridge.

use std::net::SocketAddr;

use scsynth_client_bridge::{Action, Config, parse_args};

fn parse(args: &[&str]) -> Result<Action, String> {
    parse_args(args.iter().map(|arg| (*arg).to_owned()))
}

fn addr(text: &str) -> SocketAddr {
    text.parse().unwrap()
}

#[test]
fn the_defaults_listen_on_the_loopback_address() {
    let expected = Config {
        listen: addr("127.0.0.1:57120"),
        scsynth: addr("127.0.0.1:57110"),
        origins: vec!["http://localhost:5173".to_owned()],
    };
    assert_eq!(parse(&[]), Ok(Action::Run(expected)));
}

#[test]
fn each_flag_sets_its_value() {
    let args = [
        "--listen",
        "0.0.0.0:9000",
        "--scsynth",
        "[::1]:57341",
        "--origin",
        "http://localhost:4173",
    ];
    let expected = Config {
        listen: addr("0.0.0.0:9000"),
        scsynth: addr("[::1]:57341"),
        origins: vec!["http://localhost:4173".to_owned()],
    };
    assert_eq!(parse(&args), Ok(Action::Run(expected)));
}

#[test]
fn origins_add_up_in_place_of_the_default() {
    let args = [
        "--origin",
        "http://a.example",
        "--origin",
        "http://b.example",
    ];
    let Ok(Action::Run(config)) = parse(&args) else {
        panic!("{:?}", parse(&args));
    };
    assert_eq!(config.origins, ["http://a.example", "http://b.example"]);
}

#[test]
fn help_asks_for_the_usage() {
    assert_eq!(parse(&["--help"]), Ok(Action::Help));
    assert_eq!(
        parse(&["--listen", "127.0.0.1:1", "--help"]),
        Ok(Action::Help)
    );
}

#[test]
fn an_unknown_argument_is_an_error() {
    for args in [&["-h"][..], &["--listen=127.0.0.1:1"], &["57110"]] {
        assert_eq!(parse(args), Err(format!("unknown argument {}", args[0])));
    }
}

#[test]
fn a_flag_needs_its_value() {
    assert_eq!(
        parse(&["--listen"]),
        Err("--listen needs an address".to_owned())
    );
    assert_eq!(
        parse(&["--scsynth"]),
        Err("--scsynth needs an address".to_owned())
    );
    assert_eq!(
        parse(&["--origin"]),
        Err("--origin needs an origin".to_owned())
    );
}

#[test]
fn an_address_must_parse() {
    let Err(error) = parse(&["--scsynth", "localhost:57110"]) else {
        panic!("a host name parsed as an address");
    };
    assert!(error.starts_with("--scsynth localhost:57110: "), "{error}");
}
