# Builds chainchaos from source:
#
#   docker build -t chainchaos .
#   docker run --rm -p 9545:9545 chainchaos proxy --upstream http://host.docker.internal:8545
#
# Release images (ghcr.io/youhide/chainchaos) are assembled from the prebuilt
# release binaries instead; see .github/docker/Dockerfile.release.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p chainchaos

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/chainchaos /usr/local/bin/chainchaos
# Inside a container, listen on all interfaces by default.
ENV CHAINCHAOS_LISTEN=0.0.0.0:9545
EXPOSE 9545
USER 65532:65532
ENTRYPOINT ["chainchaos"]
CMD ["--help"]
