#!/usr/bin/env bash
#
# Everything that has to be true before a tag, checked rather than remembered.
#
# A release checklist that lives in somebody's head is a checklist that gets
# shorter under deadline. This one exits non-zero, and the tag does not happen
# until it exits zero.
#
# What it checks, and why each one is here rather than assumed:
#
#   1. the tree is clean, and the commit is the one being tagged
#   2. formatting, lints and the whole test suite, in both feature builds
#   3. every negative demonstration still demonstrates
#   4. the documents still name things that exist (check-docs.sh)
#   5. every committed result still backs what it is committed to back
#      (check-artifacts.sh)
#   6. the CI sweep still covers the seeds TR-3 asks for (check-ci-budget.sh)
#   7. the pinned simulator fingerprints still replay
#   8. every crate's manifest has the metadata a publish would need, whether or
#      not the publish happens
#   9. the version in every manifest matches the tag
#
# Usage: scripts/release-checklist.sh [version]
#        scripts/release-checklist.sh v1.0.0

set -uo pipefail
SCRIPT_ROOT="$(cd "$(dirname "$0")/.." && pwd)" || exit 1
cd "${KEEL_CHECKOUT:-$SCRIPT_ROOT}" || exit 1

VERSION="${1:-}"
fail=0
step() { printf '\n=== %s\n' "$*"; }
problem() {
    printf 'FAIL %s\n' "$*"
    fail=1
}
ok() { printf 'ok   %s\n' "$*"; }

# --------------------------------------------------------------- 1. the tree

step "the tree is clean"
if [ -n "$(git status --porcelain)" ]; then
    problem "the working tree has uncommitted changes; a tag would name a commit that is not what was tested"
    git status --short | sed 's/^/     /'
else
    ok "clean at $(git rev-parse --short HEAD)"
fi

# ------------------------------------------------------------ 2. the gate

step "formatting"
if cargo fmt --all --check >/dev/null 2>&1; then ok "cargo fmt"; else problem "cargo fmt --all --check"; fi

step "lints, both feature builds"
ALL_FEATURES_TARGET="${CARGO_TARGET_DIR:-target}/clippy-all-features"
# Keep the deliberately broken `negative-demos` binaries away from the normal
# target directory. Cargo otherwise leaves `target/debug/keel-server` built with
# those workspace-unified features, and real-cluster tests launch that path.
if CARGO_TARGET_DIR="$ALL_FEATURES_TARGET" \
    cargo clippy --workspace --all-targets --all-features -- -D warnings >/dev/null 2>&1; then
    ok "clippy --all-features"
else
    problem "cargo clippy --workspace --all-targets --all-features"
fi
if cargo clippy --workspace --all-targets -- -D warnings >/dev/null 2>&1; then
    ok "clippy, default features"
else
    problem "cargo clippy --workspace --all-targets"
fi

step "tests"
# `--all-features` is deliberately invalid here: it unifies the
# `negative-demos` features into keel-server and then asks the real-cluster
# acceptance tests to pass with consensus and durability rules compiled out.
# Exercise every positive optional feature explicitly instead.
# The real-cluster tests execute this binary by path. A dev-dependency builds
# keel-server's library but does not promise to refresh that executable.
if cargo build -p keel-server >/dev/null 2>&1; then
    ok "keel-server executable"
else
    problem "cargo build -p keel-server"
fi
if cargo test --workspace >/dev/null 2>&1; then
    ok "cargo test, default features"
else
    problem "cargo test --workspace"
fi
for suite in \
    "keel-log conformance" \
    "keel-net tcp,conformance" \
    "keel-node lsm" \
    "keel-sm lsm,conformance" \
    "lsm_kv fuzzing"; do
    set -- $suite
    if cargo test -p "$1" --features "$2" >/dev/null 2>&1; then
        ok "$1 --features $2"
    else
        problem "cargo test -p $1 --features $2"
    fi
done

# The builds that deliberately remove a safety rule. Each has a test that
# asserts the harness catches the result, so a rule that stopped being
# load-bearing shows up here.
step "the deliberately broken builds still fail the way they should"
NEGATIVE_TARGET="${CARGO_TARGET_DIR:-target}/negative-demos"
for pkg in keel-raft keel-sm keel-fuzz; do
    if CARGO_TARGET_DIR="$NEGATIVE_TARGET" \
        cargo test -p "$pkg" --features negative-demos >/dev/null 2>&1; then
        ok "$pkg --features negative-demos"
    else
        problem "cargo test -p $pkg --features negative-demos"
    fi
done

# ------------------------------------------------------- 3. demonstrations

step "every negative demonstration still demonstrates"
for demo in scripts/negative-demos/*.sh; do
    name="$(basename "$demo" .sh)"
    recording="results/negative-demos/$name.txt"
    if [ ! -f "$recording" ]; then
        problem "$name has no recording"
        continue
    fi
    if grep -q '^PASS' "$recording"; then
        ok "$name"
    else
        problem "$recording does not record a PASS"
    fi
done

# ------------------------------------------------------------- 4-6. checkers

step "the documents name things that exist"
if scripts/check-docs.sh >/dev/null 2>&1; then ok "check-docs.sh"; else problem "scripts/check-docs.sh"; fi

step "every result backs what it is committed to back"
if scripts/check-artifacts.sh >/dev/null 2>&1; then
    ok "check-artifacts.sh"
else
    problem "scripts/check-artifacts.sh"
fi

step "the sweep covers the seeds it claims"
if scripts/check-ci-budget.sh 2000 50000 >/dev/null 2>&1; then
    ok "check-ci-budget.sh"
else
    problem "scripts/check-ci-budget.sh"
fi

# ------------------------------------------------------------ 7. determinism

step "the pinned simulator fingerprints still replay"
if cargo test -p keel-sim --test simulation \
    the_committed_profiles_still_replay_to_their_pinned_fingerprints >/dev/null 2>&1; then
    ok "18 pinned (profile, seed, fingerprint) triples"
else
    problem "the pinned fingerprints moved"
fi

# --------------------------------------------------- 8. publishable metadata

# The decision taken at P28 is that the crates are *not* published to
# crates.io at v1.0.0 — a workspace with a vendored copy of another repository
# in it is not something to put on a registry, and the API is not yet one
# anybody should depend on. The dry run stays as hygiene: a manifest missing a
# description or a licence is a manifest nobody has read, whether or not it is
# ever uploaded.
step "every manifest carries the metadata a publish would need"
for manifest in crates/*/Cargo.toml; do
    name="$(basename "$(dirname "$manifest")")"
    missing=""
    grep -q '^description' "$manifest" || missing="$missing description"
    grep -q '^license' "$manifest" || missing="$missing license"
    grep -q '^repository' "$manifest" || missing="$missing repository"
    if [ -n "$missing" ]; then
        problem "$name is missing:$missing"
    else
        ok "$name"
    fi
done

# ---------------------------------------------------------------- 9. version

if [ -n "$VERSION" ]; then
    step "the version matches the tag"
    expected="${VERSION#v}"
    actual="$(awk -F'"' '/^version/ {print $2; exit}' Cargo.toml)"
    if [ "$actual" = "$expected" ]; then
        ok "workspace version $actual"
    else
        problem "the workspace says $actual and the tag says $expected"
    fi
    # A tag that already exists is only a problem if it names a *different*
    # commit. This check read "the tag is free", which meant the checklist could
    # never pass at the tagged commit — and passing there is the whole exit
    # criterion, since the point is to verify what was actually tagged rather
    # than what was about to be.
    if existing="$(git rev-parse "$VERSION^{commit}" 2>/dev/null)"; then
        if [ "$existing" = "$(git rev-parse HEAD)" ]; then
            ok "$VERSION already points here"
        else
            problem "$VERSION exists and points at ${existing:0:7}, not at HEAD"
        fi
    else
        ok "$VERSION is free"
    fi
fi

# ---------------------------------------------------------------------------

echo
if ((fail)); then
    echo "=============================================================="
    echo "NOT READY. The checks above that say FAIL are the reason."
    exit 1
fi
echo "=============================================================="
echo "READY at $(git rev-parse --short HEAD)."
if [ -n "$VERSION" ]; then
    echo "Tag with: git tag -a $VERSION -m 'Keel $VERSION'"
fi
