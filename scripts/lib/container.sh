#!/usr/bin/env bash
#
# Which container runtime this machine has, and what each one is called here.
#
# Two scripts need a Linux container: the clock nemesis, because macOS cannot be
# lied to about `CLOCK_MONOTONIC` at all (ADR-026), and the etcd baseline,
# because etcd is not built for this host. Both were written against Docker,
# which is right for a laptop and wrong for the machine the Reference tier is
# waiting on: a shared HPC cluster does not give users a Docker daemon, because
# a Docker daemon is root. It gives them Apptainer, which runs as the user.
#
# So the runtime is detected rather than assumed. The alternative was a second
# copy of each script, which is how one of the two copies rots.
#
# **Docker and Podman are exercised; Apptainer is not.** No Apptainer exists on
# the machine these scripts were written on, so the Apptainer path is written
# from its documentation and has never run. That is stated here, in the artifact
# headers, and in BENCH.md, because a path nobody has run is a claim and not a
# measurement — and the first person to run it on a cluster should know which
# one they are holding.
#
# Sourced, not run.

# Set `KEEL_CONTAINER` to the runtime to use, or return non-zero.
#
# Docker first because it is what the committed artifacts were produced with, so
# a machine with both reproduces them rather than producing something new.
container_detect() {
    if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
        KEEL_CONTAINER=docker
        return 0
    fi
    if command -v podman >/dev/null && podman info >/dev/null 2>&1; then
        KEEL_CONTAINER=podman
        return 0
    fi
    # Apptainer was Singularity; a cluster may have either name on the path.
    for name in apptainer singularity; do
        if command -v "$name" >/dev/null; then
            KEEL_CONTAINER="$name"
            return 0
        fi
    done
    KEEL_CONTAINER=""
    return 1
}

# Whether the detected runtime is one of the daemon-and-images pair.
container_is_oci() {
    [ "$KEEL_CONTAINER" = docker ] || [ "$KEEL_CONTAINER" = podman ]
}

# One line for an artifact header, naming the runtime and its version.
container_header() {
    case "$KEEL_CONTAINER" in
        docker | podman)
            echo "runtime:         $KEEL_CONTAINER $("$KEEL_CONTAINER" --version 2>/dev/null | head -1)"
            ;;
        apptainer | singularity)
            echo "runtime:         $KEEL_CONTAINER $("$KEEL_CONTAINER" --version 2>/dev/null | head -1)"
            echo "                 (this path has never been run on the machine that wrote it;"
            echo "                  see scripts/lib/container.sh)"
            ;;
        *) echo "runtime:         none" ;;
    esac
}

# What to tell somebody who has none of them.
container_missing() {
    cat >&2 <<'MISSING'
no container runtime found. This needs one of:

  docker      a running daemon
  podman      rootless, and a drop-in for docker here
  apptainer   what a shared cluster gives you instead of either
              (or singularity, its former name)

On a cluster, `module load apptainer` usually puts it on the path.
MISSING
}
