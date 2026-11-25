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
