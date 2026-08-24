# The node binary, built from this tree.
#
# Two stages so the image that runs is not the image that has a Rust toolchain
# in it. The vendored storage engine builds with everything else; there is no
# separate step because there is no separate repository at build time.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p keel-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/keel-server /usr/local/bin/keel-server
# The data directory is a volume, so a node that is restarted comes back with
# its log — which is the point of the log.
VOLUME /data
ENTRYPOINT ["/usr/local/bin/keel-server"]
