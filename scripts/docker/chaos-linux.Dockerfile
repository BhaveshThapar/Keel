# The clock nemesis needs a Linux the harness is allowed to lie to.
#
# macOS strips DYLD_INSERT_LIBRARIES from any process that execs a protected
# binary, and even where the preload survives, dyld interposition does not reach
# the commpage that mach_absolute_time reads. So libfaketime cannot move
# CLOCK_MONOTONIC there at all — not slowly, not partially, not at all. This
# image exists so that one fault is injected somewhere it can be, rather than
# skipped with a note.
#
# Built and run by scripts/chaos-clock.sh.

FROM rust:1-slim-bookworm

# faketime brings libfaketime.so, which is the whole point of the image.
# pkg-config and libclang are what the vendored storage engine's build needs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        faketime \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

# A separate target directory: the host's is full of Mach-O and sharing it would
# make every switch between the two a full rebuild, in both directions.
ENV CARGO_TARGET_DIR=/work/target-linux

CMD ["/bin/bash"]
