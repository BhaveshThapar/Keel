#!/usr/bin/env bash
#
# A committed result has to say where it came from, and it has to still be true.
#
# The rule the repository works to is that no claim outruns its artifact. That
# only means anything if the artifacts themselves are checked: a file under
# results/ with no provenance is a number with no hardware and no commit behind
# it, and a file recorded before a commit that changed the code it measures is a
# number about a program that no longer exists.
#
# Four checks:
#   1. every file under results/ carries host/commit/date
#   2. the commit it names is a real commit, and an ancestor of HEAD
#   3. it was not recorded from a modified working tree
#   4. every negative demonstration has a recording, and every recording has a
#      demonstration
#
# Usage: scripts/check-artifacts.sh

set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
problem() {
    printf 'FAIL %s\n' "$*"
    fail=1
}

# ------------------------------------------------- 1-3. provenance and age

count=0
while read -r artifact; do
    count=$((count + 1))
    # The header is at the top, under a title line or two. Ten lines is generous
    # and still refuses a file that carries the fields somewhere in its middle.
    header="$(head -10 "$artifact")"

    commit_line="$(grep -m1 '^commit:' <<<"$header" || true)"
    if [[ -z "$commit_line" ]] ||
        ! grep -q '^host:' <<<"$header" ||
        ! grep -q '^date:' <<<"$header"; then
        problem "$artifact has no provenance header (host, commit, date)"
        continue
    fi

    if grep -q 'working tree modified' <<<"$commit_line"; then
        problem "$artifact was recorded from a modified tree, so its commit does not identify what ran"
        continue
    fi

    sha="$(awk '{print $2}' <<<"$commit_line")"
    if ! git cat-file -e "$sha^{commit}" 2>/dev/null; then
        problem "$artifact names commit $sha, which is not a commit in this repository"
        continue
    fi
    # An artifact from a commit that is not behind HEAD was recorded on a branch
    # that was never merged, or on a commit that was rewritten. Either way it no
    # longer describes this tree.
    if ! git merge-base --is-ancestor "$sha" HEAD 2>/dev/null; then
        problem "$artifact names commit $sha, which is not an ancestor of HEAD"
    fi
done < <(git ls-files 'results/*')

if ((count == 0)); then
    problem "no artifacts are tracked under results/; nothing was checked"
fi

# ------------------------------------------- 4. demonstrations and recordings

# The Maelstrom run is not a demonstration and has no control arm, so it is
# checked for provenance like any other artifact and exempted from the PASS
# rule below only by living outside results/negative-demos/.
for demo in scripts/negative-demos/*.sh; do
    name="$(basename "$demo" .sh)"
    [[ -f "results/negative-demos/$name.txt" ]] ||
        problem "$demo has no recorded run at results/negative-demos/$name.txt"
done

for recording in results/negative-demos/*.txt; do
    [[ -e "$recording" ]] || continue
    name="$(basename "$recording" .txt)"
    [[ -f "scripts/negative-demos/$name.sh" ]] ||
        problem "$recording records a demonstration that no longer exists"
done

# A recorded demonstration that says FAIL is a demonstration that stopped
# demonstrating, however clean the rest of the tree is.
while read -r recording; do
    grep -q '^PASS' "$recording" ||
        problem "$recording does not record a PASS"
done < <(git ls-files 'results/negative-demos/*.txt')

# ---------------------------------------------------------------------------

if ((fail)); then
    echo
    echo "An artifact does not back what it is committed to back."
    exit 1
fi
echo "artifact check clean ($count tracked)"
