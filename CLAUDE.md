# scsynth-client

A Rust crate that is a client of scsynth, the synthesis server of
SuperCollider. It sends the typed requests of scsynth-osc, and waits for
the replies of scsynth, over UDP, TCP, a websocket or a message port. It
takes scsynth-osc and scsynth-synthdef by git tag. This file is the
operating manual.

The README is the reference of the crate. `docs/plan.md` will hold what is
still open after version 0.1. Read the README before any work.

## Ground rules

- A fact about scsynth comes from its source or from a measurement with
  scsynth 3.14.1. The SuperCollider source is in `~/Projects/supercollider`,
  at the tag `Version-3.14.1`. The scsynth of the SuperCollider bundle is
  `/Applications/SuperCollider.app/Contents/Resources/scsynth`.
- Never touch the scsynth on UDP 57110. A probe of a real scsynth runs its
  own scsynth on another port.
- Make no change to the sibling crates scsynth-osc and scsynth-synthdef
  from this repository.
- Every dependency from crates.io must be at least 7 days old. The sibling
  crates come by git tag, so the rule does not cover them.

## Commands

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features
RUSTDOCFLAGS=-Dwarnings cargo doc --no-deps --all-features
cargo clippy --all-features --target wasm32-unknown-unknown -- -D warnings
cargo +1.85.0 test --all-features    # the MSRV
cargo deny check
```

CI (`.github/workflows/ci.yml`) runs them with `--locked`. CI has no
`cargo package` step, because the sibling crates are git dependencies.

The tests of `tests/scsynth.rs` are ignored by default, because they need
a real scsynth. CI has no scsynth, so it does not run them. Run them by
hand against a scsynth of their own, with no JACK connection, and quit it
after:

```bash
SC_JACK_DEFAULT_INPUTS="" SC_JACK_DEFAULT_OUTPUTS="" \
  /Applications/SuperCollider.app/Contents/Resources/scsynth -u 57341 -t 57341 -B 127.0.0.1 &
SCSYNTH_ADDR=127.0.0.1:57341 cargo test --all-features --test scsynth -- --ignored
```

That scsynth needs the running jackd. The README says what the tests use
on the server, under Tests. After a failed run, restart that scsynth.

`examples/bridge` is a crate of its own, with its own `Cargo.lock`, and
the commands above do not reach it. Its commands, which the CI job
`bridge` runs with `--locked`:

```bash
cargo fmt --check --manifest-path examples/bridge/Cargo.toml
cargo clippy --all-targets --manifest-path examples/bridge/Cargo.toml -- -D warnings
cargo test --manifest-path examples/bridge/Cargo.toml
cargo deny --manifest-path examples/bridge/Cargo.toml --config deny.toml check
```

Its test on a real scsynth is ignored by default too. Run it against a
private scsynth, as above:

```bash
SCSYNTH_ADDR=127.0.0.1:57341 cargo test --manifest-path examples/bridge/Cargo.toml -- --ignored
```

`examples/vite` is the Vite page, with its wasm crate in
`examples/vite/wasm`, which has its own `Cargo.lock`. Its commands, which
the CI job `example` runs with `--locked`:

```bash
cargo fmt --check --manifest-path examples/vite/wasm/Cargo.toml
cargo clippy --manifest-path examples/vite/wasm/Cargo.toml --target wasm32-unknown-unknown -- -D warnings
cd examples/vite && npm ci && npm run wasm && npm run build
```

The page plays a sine. Run it against a private scsynth with no JACK
connection, as above, with `-l 32`, through the bridge:

```bash
cargo run --manifest-path examples/bridge/Cargo.toml -- --scsynth 127.0.0.1:57341 --listen 127.0.0.1:57342 --origin http://localhost:5199
cd examples/vite && npm run dev -- --port 5199 --strictPort
```

Then open `http://localhost:5199` and connect to `ws://127.0.0.1:57342`.

## Conventions

- Edition 2024, MSRV 1.85, clippy pedantic, no unsafe code, docs on every
  public item. CI runs clippy with `-D warnings`.
- A comment states a fact that the code cannot: a measured number, a trap,
  a constraint. Design rationale goes in the README.
- One fact, one owner. The owner is the test that breaks with the fact.
  Without a test, the owner is the file where the edit goes.
- Work goes on a branch, with one commit per step of the plan. Every commit
  leaves fmt, clippy, the tests and the docs green. A branch reaches `main`
  through a GitHub PR.
