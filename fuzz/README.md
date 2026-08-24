# Running a real campaign

The targets themselves are in [`crates/keel-fuzz`](../crates/keel-fuzz), as
ordinary functions over `&[u8]`. This directory is the libFuzzer wiring, and it
is eight lines per target.

```
rustup toolchain install nightly
cargo install cargo-fuzz
cargo +nightly fuzz run api_proposal
```

Nothing in CI builds this, and that is the decision rather than an oversight:
`cargo-fuzz` needs nightly for `-Z sanitizer=address`, and this repository pins
stable in `rust-toolchain.toml` for everything else. Splitting the toolchain to
run a fuzzer nobody would look at between releases buys less than running the
same targets on stable on every commit, which is what
`crates/keel-fuzz`'s smoke harness does. See ADR-029.

What the smoke harness cannot do is what coverage guidance is for: keep the
inputs that reached new code and mutate those. When a campaign is worth running
— before a release, or after a parser changes — it is one command, and any
crash it finds is a file under `artifacts/` that
`keel_fuzz::<target>(&std::fs::read(path)?)` replays on stable.
