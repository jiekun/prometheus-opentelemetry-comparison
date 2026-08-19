BACKEND_DIR          := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
BACKEND_BIN_DIR      := $(abspath $(BACKEND_DIR)../../../bin)
BACKEND_ROOT_DIR             := $(abspath $(BACKEND_DIR)../../..)

PROM_SERVER_VERSION   ?= 3.14.0
PROM_SERVER_TARBALL   := prometheus-$(PROM_SERVER_VERSION).linux-amd64.tar.gz
PROM_SERVER_BIN       := $(BACKEND_BIN_DIR)/prometheus
PROM_SERVER_CONFIG    := $(BACKEND_DIR)prometheus.yaml
PROM_SERVER_DATA_DIR  := $(BACKEND_BIN_DIR)/data
PROM_SERVER_LOG       := $(BACKEND_BIN_DIR)/prometheus_server.log
PROM_SERVER_PID       := $(BACKEND_BIN_DIR)/prometheus_server.pid

NOOP_RECEIVER_BIN     := $(BACKEND_BIN_DIR)/metrics-noop-receiver
NOOP_RECEIVER_LOG     := $(BACKEND_BIN_DIR)/metrics-noop-receiver.log
NOOP_RECEIVER_PID     := $(BACKEND_BIN_DIR)/metrics-noop-receiver.pid

.PHONY: init-prometheus-server run-prometheus-server stop-prometheus-server \
	noop-receiver run-noop-receiver stop-noop-receiver

# Downloads the prometheus binary used by run-prometheus-server. Safe to
# re-run on the same instance.
init-prometheus-server:
	mkdir -p $(BACKEND_BIN_DIR)
	wget -q https://github.com/prometheus/prometheus/releases/download/v$(PROM_SERVER_VERSION)/$(PROM_SERVER_TARBALL) -O /tmp/$(PROM_SERVER_TARBALL)
	tar -C $(BACKEND_BIN_DIR) --strip-components=1 -xzf /tmp/$(PROM_SERVER_TARBALL) prometheus-$(PROM_SERVER_VERSION).linux-amd64/prometheus
	rm -f /tmp/$(PROM_SERVER_TARBALL)

# Runs Prometheus as a full backend (local TSDB + query API, not agent
# mode): no scrape targets, metrics only arrive by being pushed to it via
# remote-write and OTLP (see docker-compose.prometheus.yaml).
run-prometheus-server:
	mkdir -p $(PROM_SERVER_DATA_DIR)
	nohup $(PROM_SERVER_BIN) --config.file=$(PROM_SERVER_CONFIG) --storage.tsdb.path=$(PROM_SERVER_DATA_DIR) --web.enable-remote-write-receiver --web.enable-otlp-receiver > $(PROM_SERVER_LOG) 2>&1 & echo $$! > $(PROM_SERVER_PID)

stop-prometheus-server:
	@if [ -f $(PROM_SERVER_PID) ]; then \
		xargs -r kill < $(PROM_SERVER_PID); \
		rm -f $(PROM_SERVER_PID); \
		echo "stopped prometheus-server"; \
	else \
		echo "no $(PROM_SERVER_PID) file found, nothing to stop"; \
	fi

# Builds cmd/metrics-noop-receiver: a stand-in "Prometheus server" that
# accepts Prometheus remote-write v1/v2 (POST /api/v1/write, /api/v2/write)
# and OTLP (POST /otlp/export over HTTP, or gRPC on :8001), decodes each
# request and discards it rather than storing it, and exposes
# decode_error_total/read_error_total plus its own resource usage on
# :8000/metrics (see internal/receiver). Useful as a zero-storage-overhead
# baseline to compare a real Prometheus server against. Ports (8000 HTTP,
# 8001 gRPC) are hardcoded in cmd/metrics-noop-receiver/main.go, not
# configurable via env/flags.
noop-receiver:
	go build -C $(BACKEND_ROOT_DIR) -o $(NOOP_RECEIVER_BIN) ./cmd/metrics-noop-receiver

run-noop-receiver: noop-receiver
	nohup $(NOOP_RECEIVER_BIN) > $(NOOP_RECEIVER_LOG) 2>&1 & echo $$! > $(NOOP_RECEIVER_PID)

stop-noop-receiver:
	@if [ -f $(NOOP_RECEIVER_PID) ]; then \
		xargs -r kill < $(NOOP_RECEIVER_PID); \
		rm -f $(NOOP_RECEIVER_PID); \
		echo "stopped noop-receiver"; \
	else \
		echo "no $(NOOP_RECEIVER_PID) file found, nothing to stop"; \
	fi

