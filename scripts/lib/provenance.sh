#!/usr/bin/env bash
#
# The header every artifact under results/ carries.
#
# Sourced, never executed. Factored out of sweep.sh so a second script cannot
# quietly record a run with less provenance than the first one did — which is
# what happened to results/negative-demos/figure-8.txt, captured by hand with no
# header at all.

# The host belongs in the artifact, and it has to come from the machine rather
# than from whoever committed the file. `uname` alone does not say what the CPU
# is, which is the part a reader wants.
host_line() {
    case "$(uname -s)" in
        Darwin)
            printf '%s, %s cores, %s GiB, macOS %s, %s\n' \
                "$(sysctl -n machdep.cpu.brand_string)" \
                "$(sysctl -n hw.ncpu)" \
                "$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))" \
                "$(sw_vers -productVersion)" \
                "$(uname -m)"
            ;;
        Linux)
            printf '%s, %s cores, %s GiB, Linux %s, %s\n' \
                "$(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo)" \
                "$(nproc)" \
                "$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))" \
                "$(uname -r)" \
                "$(uname -m)"
            ;;
        *) printf '%s %s, %s\n' "$(uname -s)" "$(uname -r)" "$(uname -m)" ;;
    esac
}

# Read the tree's state *before* the pipeline that writes the artifact starts.
# `tee` truncates the output the moment it opens, and the artifact is tracked,
# so a check made inside the block always reports a modified tree — including
# when the tree is clean. The provenance line is the whole point of these files,
# so it may not be measuring its own side effect.
#
# An untracked file is a modification too. `git diff` cannot see one, so a brand
# new script — exactly what a brand new artifact is usually produced by — would
# otherwise record itself as having run against a clean tree.
#
# The whole of results/ is excluded rather than just this artifact. The flag
# exists to say whether the code that ran is the code at this commit, and a
# recording is not code: a script that records several artifacts in one pass
# would otherwise have every file after the first report a modified tree,
# because the earlier ones had just been written. An artifact that has drifted
# away from the commit it names is caught by check-artifacts.sh instead, which
# is the check that can actually tell.
#
# Call as: provenance_of results/simulator/sweep.txt
provenance_of() {
    # The exclusion above is written as results/, so an artifact somewhere else
    # would be measuring its own side effect again and quietly.
    case "$1" in
        results/*) ;;
        *)
            echo "provenance_of: $1 is not under results/" >&2
            return 1
            ;;
    esac
    HEAD_SHA="$(git rev-parse --short HEAD)"
    DIRTY=""
    git diff --quiet -- . ':(exclude)results' || DIRTY=" (working tree modified)"
    git diff --quiet --cached -- . ':(exclude)results' || DIRTY=" (working tree modified)"
    if git ls-files --others --exclude-standard -- . ':(exclude)results' | grep -q .; then
        DIRTY=" (working tree modified)"
    fi
}

# Call inside the block that writes the artifact, after provenance_of.
provenance_header() {
    echo "host:   $(host_line)"
    echo "commit: ${HEAD_SHA}${DIRTY}"
    echo "date:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
