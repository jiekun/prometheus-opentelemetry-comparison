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

# Same release train as VMAGENT_VERSION in docker/agent/Makefile - keep
# the two in sync if you bump one.
VM_SERVER_VERSION   ?= 1.150.0
VM_SERVER_TARBALL   := victoria-metrics-linux-amd64-v$(VM_SERVER_VERSION).tar.gz
VM_SERVER_BIN       := $(BACKEND_BIN_DIR)/victoria-metrics
VM_SERVER_DATA_DIR  := $(BACKEND_BIN_DIR)/vm-data
VM_SERVER_LOG       := $(BACKEND_BIN_DIR)/victoria_metrics_server.log
VM_SERVER_PID       := $(BACKEND_BIN_DIR)/victoria_metrics_server.pid

NOOP_RECEIVER_BIN     := $(BACKEND_BIN_DIR)/metrics-noop-receiver
NOOP_RECEIVER_LOG     := $(BACKEND_BIN_DIR)/metrics-noop-receiver.log
NOOP_RECEIVER_PID     := $(BACKEND_BIN_DIR)/metrics-noop-receiver.pid

.PHONY: init-prometheus-server run-prometheus-server stop-prometheus-server \
	init-victoria-metrics run-victoria-metrics stop-victoria-metrics \
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

# Downloads the victoria-metrics binary used by run-victoria-metrics. Safe
# to re-run on the same instance. Check
# https://github.com/VictoriaMetrics/VictoriaMetrics/releases for the
# latest version and the exact asset name before bumping
# VM_SERVER_VERSION - not pinned as carefully here as PROM_SERVER_VERSION
# above.
init-victoria-metrics:
	mkdir -p $(BACKEND_BIN_DIR)
	wget -q https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v$(VM_SERVER_VERSION)/$(VM_SERVER_TARBALL) -O /tmp/$(VM_SERVER_TARBALL)
	tar -C $(BACKEND_BIN_DIR) -xzf /tmp/$(VM_SERVER_TARBALL) victoria-metrics-prod
	mv $(BACKEND_BIN_DIR)/victoria-metrics-prod $(VM_SERVER_BIN)
	rm -f /tmp/$(VM_SERVER_TARBALL)

# Runs VictoriaMetrics single-node as a full backend (local storage +
# query API), the same role run-prometheus-server plays above: no scrape
# targets, no config file - metrics only arrive by being pushed to it.
# Unlike Prometheus, VictoriaMetrics accepts both Prometheus remote-write
# (/api/v1/write) and OTLP (/opentelemetry/v1/metrics) unconditionally, no
# --web.enable-* flags needed, so this same instance can be pointed at by
# either prometheus-agent/vmagent or otel-agent depending on which
# ingestion path you're comparing. Default listen port is 8428 (vs
# Prometheus's 9090).
run-victoria-metrics:
	mkdir -p $(VM_SERVER_DATA_DIR)
	nohup $(VM_SERVER_BIN) -storageDataPath=$(VM_SERVER_DATA_DIR) > $(VM_SERVER_LOG) 2>&1 & echo $$! > $(VM_SERVER_PID)

stop-victoria-metrics:
	@if [ -f $(VM_SERVER_PID) ]; then \
		xargs -r kill < $(VM_SERVER_PID); \
		rm -f $(VM_SERVER_PID); \
		echo "stopped victoria-metrics"; \
	else \
		echo "no $(VM_SERVER_PID) file found, nothing to stop"; \
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

