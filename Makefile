.PHONY: build run test fmt check clippy helm-lint helm-template image

VERSION ?= dev
IMAGE ?= ghcr.io/rushobservability/metrics-agent:$(VERSION)
LOCAL_QUERY_API_REMOTE_WRITE_URL ?= http://localhost:8080/prom/api/v1/write
RUSH_REMOTE_WRITE_URL ?= http://rush-query-api.monitoring.svc:8080/prom/api/v1/write

build:
	cargo build --release

run: RUSH_REMOTE_WRITE_URL = $(LOCAL_QUERY_API_REMOTE_WRITE_URL)
run: RUSH_REMOTE_WRITE_TENANT = default
run:
	RUSH_REMOTE_WRITE_URL="$(RUSH_REMOTE_WRITE_URL)" \
	RUSH_REMOTE_WRITE_TENANT="$(RUSH_REMOTE_WRITE_TENANT)" \
	cargo run --bin metrics-agent

test:
	cargo test

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

check: fmt test
	cargo check

helm-lint:
	helm lint helm-chart --set rushRemoteWrite.enabled=true --set 'rushRemoteWrite.url=$(RUSH_REMOTE_WRITE_URL)'

helm-template:
	helm template metrics-agent helm-chart --namespace monitoring --set rushRemoteWrite.enabled=true --set 'rushRemoteWrite.url=$(RUSH_REMOTE_WRITE_URL)' >/dev/null

verify: fmt test helm-lint helm-template
	cargo check

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .
