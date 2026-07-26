# syntax=docker/dockerfile:1.7
ARG CHAINGUARD_RUST_IMAGE=cgr.dev/chainguard/rust:latest
ARG CHAINGUARD_RUNTIME_IMAGE=cgr.dev/chainguard/glibc-dynamic:latest

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
