#!/usr/bin/env bash
#
# The documents name things. This checks the things still exist.
#
# CORRECTNESS.md is a table of property -> the test that enforces it, DESIGN.md
# is a set of numbered ADRs other files cite, and BUGS.md is a set of numbered
# defects other files cite. Every one of those references can go stale silently:
# a renamed test leaves a row that reads as enforcement and enforces nothing,
# which is worse than a row that was never written.
#
# Four checks:
#   1. every test CORRECTNESS.md names exists in the workspace test list
#   2. every ADR-0NN cited anywhere resolves to a heading in DESIGN.md
#   3. every KEEL-N cited anywhere resolves to a heading in BUGS.md
#   4. every relative markdown link resolves to a path that exists
#
# Usage: scripts/check-docs.sh

set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
note() { printf '  %s\n' "$*"; }
problem() {
    printf 'FAIL %s\n' "$*"
    fail=1
}

# Every tracked markdown file, greppable in one pass. Nul-separated, because a
# filename is allowed to contain a space and a checker that breaks on one is a
# checker that will be disabled rather than fixed.
grep_docs() { git ls-files -z '*.md' | xargs -0 grep "$@"; }

# ---------------------------------------------------------------- 1. tests

# Every test the workspace can run, including the ones that only exist when a
# safety rule has been compiled out — CORRECTNESS.md cites one of those by name
# and it would otherwise read as a stale row on every run.
tests_file="$(mktemp)"
trap 'rm -f "$tests_file"' EXIT
{
    cargo test --workspace --all-targets -- --list 2>/dev/null
    cargo test -p keel-raft --features negative-demos --all-targets -- --list 2>/dev/null
} | sed -n 's/: test$//p' | sort -u >"$tests_file"

if [[ ! -s "$tests_file" ]]; then
    problem "the workspace test list came back empty; nothing was checked"
    exit 1
fi
note "$(wc -l <"$tests_file" | tr -d ' ') tests in the workspace"

# A unit test lists as `module::tests::name`; an integration test lists as just
# `name`, because the binary it lives in is not part of what libtest prints.
# CORRECTNESS.md writes the binary name anyway — `recovery::name` — since that
# is how a reader finds the file. So a citation resolves if libtest knows it
# whole, or knows it with the leading segment dropped.
resolves_as_test() {
    local name="$1"
    grep -qxF "$name" "$tests_file" && return 0
    [[ "$name" == *::* ]] && grep -qxF "${name#*::}" "$tests_file" && return 0
    return 1
}

# Not every backticked lowercase identifier is a test. `write_at` is a trait
# method, `applied_index` is a key, `keel_log::conformance::check` is the
# function a conformance suite exposes. Those still have to exist, so the
# fallback is the source tree rather than an exemption list that would have to
# be maintained by hand.
resolves_in_source() {
    local leaf="${1##*::}"
    grep -rqw --include='*.rs' --include='*.sh' -- "$leaf" crates scripts
}

# shellcheck disable=SC2016 # the backticks are the thing being matched, not a variable
while read -r name; do
    [[ -z "$name" ]] && continue
    if ! resolves_as_test "$name" && ! resolves_in_source "$name"; then
        problem "CORRECTNESS.md names \`$name\`, which is neither a test nor anything in the tree"
    fi
done < <(grep -o '`[^`]*`' CORRECTNESS.md | tr -d '`' |
    grep -E '^[a-z][a-z0-9_]*(::[a-z][a-z0-9_]*)*$' | sort -u)

# ------------------------------------------------------------------ 2. ADRs

while read -r adr; do
    grep -qE "^## $adr\b|^- \*\*$adr\b" DESIGN.md ||
        problem "$adr is cited but DESIGN.md has no such ADR"
done < <(grep_docs -ohE 'ADR-[0-9]+' | sort -u)

# ------------------------------------------------------------------ 3. bugs

while read -r bug; do
    grep -qE "^## $bug\b" BUGS.md ||
        problem "$bug is cited but BUGS.md has no such entry"
done < <(grep_docs -ohE 'KEEL-[0-9]+' | sort -u)

# ----------------------------------------------------------------- 4. links

while read -r doc; do
    while read -r target; do
        # In-page anchors and absolute URLs are somebody else's problem.
        [[ "$target" == \#* || "$target" == http* || "$target" == mailto:* ]] && continue
        # A line anchor names a line, not a file. The file is what can vanish.
        path="${target%%#*}"
        [[ -z "$path" ]] && continue
        # Links are written relative to the file that carries them.
        resolved="$(dirname "$doc")/$path"
        [[ -e "$resolved" ]] ||
            problem "$doc links to $target, which does not exist"
        # Fenced code is skipped. A transcript pasted into a bug report is not
        # markup, and `voters=[1, 5](joint)` in one is not a broken link — but
        # it looks exactly like one to a regular expression, and a checker that
        # cries wolf about evidence is a checker somebody edits the evidence to
        # satisfy.
    done < <(awk '/^```/ { fenced = !fenced; next } !fenced' "$doc" |
        grep -ohE '\]\([^)]+\)' | sed -E 's/^\]\(//; s/\)$//')
done < <(git ls-files "*.md")

# ---------------------------------------------------------------------------

if ((fail)); then
    echo
    echo "A document is claiming something the tree does not back."
    exit 1
fi
echo "docs check clean"
