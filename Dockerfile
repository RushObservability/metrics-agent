# syntax=docker/dockerfile:1.7
ARG CHAINGUARD_RUST_IMAGE=cgr.dev/chainguard/rust:latest@sha256:6c42dfb2cad5356d7c043155f896c5bd8e8777377ff21b6c78ee2113f6ee092d
ARG CHAINGUARD_RUNTIME_IMAGE=cgr.dev/chainguard/glibc-dynamic:latest@sha256:d0046044cd28948d3380eb0d98709dc7e63f98161fe7105135e1025650bad17a

FROM ${CHAINGUARD_RUST_IMAGE} AS build

WORKDIR /tmp/metrics-agent
COPY --chown=65532:65532 Cargo.toml Cargo.lock ./
COPY --chown=65532:65532 src ./src
COPY --chown=65532:65532 ui ./ui
ARG VERSION=dev
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked && \
    cp target/release/metrics-agent /tmp/out-metrics-agent

FROM ${CHAINGUARD_RUNTIME_IMAGE}
ARG VERSION=dev
LABEL org.opencontainers.image.version=${VERSION}
COPY --from=build --chown=65532:65532 /tmp/out-metrics-agent /usr/local/bin/metrics-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/metrics-agent"]
