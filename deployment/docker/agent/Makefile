AGENT_DIR         := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
AGENT_BIN_DIR     := $(abspath $(AGENT_DIR)../../../bin)

OTEL_VERSION      ?= 0.159.0
OTEL_TARBALL      := otelcol-contrib_$(OTEL_VERSION)_linux_amd64.tar.gz
OTEL_BIN          := $(AGENT_BIN_DIR)/otelcol-contrib
OTEL_CONFIG       := $(AGENT_DIR)opentelemetry-collector.yaml
OTEL_LOG          := $(AGENT_BIN_DIR)/otel_agent.log
OTEL_PID          := $(AGENT_BIN_DIR)/otel_agent.pid

PROM_AGENT_VERSION   ?= 3.14.0
PROM_AGENT_TARBALL   := prometheus-$(PROM_AGENT_VERSION).linux-amd64.tar.gz
PROM_AGENT_BIN       := $(AGENT_BIN_DIR)/prometheus
PROM_AGENT_CONFIG    := $(AGENT_DIR)prometheus-agent.yaml
PROM_AGENT_DATA_DIR  := $(AGENT_BIN_DIR)/prometheus-agent-data
PROM_AGENT_LOG       := $(AGENT_BIN_DIR)/prometheus_agent.log
PROM_AGENT_PID       := $(AGENT_BIN_DIR)/prometheus_agent.pid

# Check https://github.com/VictoriaMetrics/VictoriaMetrics/releases for the
# latest version and the exact vmutils asset name before running
# init-vmagent - both drift over time and aren't pinned as carefully here
# as OTEL_VERSION/PROM_AGENT_VERSION above.
VMAGENT_VERSION          ?= 1.150.0
VMAGENT_TARBALL          := vmutils-linux-amd64-v$(VMAGENT_VERSION).tar.gz
VMAGENT_BIN              := $(AGENT_BIN_DIR)/vmagent
VMAGENT_CONFIG           := $(AGENT_DIR)vmagent.yaml
VMAGENT_DATA_DIR         := $(AGENT_BIN_DIR)/vmagent-data
VMAGENT_LOG              := $(AGENT_BIN_DIR)/vmagent.log
VMAGENT_PID              := $(AGENT_BIN_DIR)/vmagent.pid
# Same host:port cmd/metrics-noop-receiver as prometheus-agent.yaml's
# remote_write, but /api/v1/write rather than /api/v2/write - see
# vmagent.yaml's own comment for why.
VMAGENT_REMOTE_WRITE_URL ?= http://10.140.15.206:9090/api/v1/write

.PHONY: init-otel-agent run-otel-agent stop-otel-agent \
	init-prometheus-agent run-prometheus-agent stop-prometheus-agent \
	init-vmagent run-vmagent stop-vmagent

# Downloads the otelcol-contrib binary used by run-otel-agent. Safe to
# re-run on the same instance.
init-otel-agent:
	mkdir -p $(AGENT_BIN_DIR)
	wget -q https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$(OTEL_VERSION)/$(OTEL_TARBALL) -O /tmp/$(OTEL_TARBALL)
	tar -C $(AGENT_BIN_DIR) -xzf /tmp/$(OTEL_TARBALL) otelcol-contrib
	rm -f /tmp/$(OTEL_TARBALL)

run-otel-agent:
	nohup $(OTEL_BIN) --config $(OTEL_CONFIG) > $(OTEL_LOG) 2>&1 & echo $$! > $(OTEL_PID)

stop-otel-agent:
	@if [ -f $(OTEL_PID) ]; then \
		xargs -r kill < $(OTEL_PID); \
		rm -f $(OTEL_PID); \
		echo "stopped otel-agent"; \
	else \
		echo "no $(OTEL_PID) file found, nothing to stop"; \
	fi

# Downloads the prometheus binary used by run-prometheus-agent. Safe to
# re-run on the same instance.
init-prometheus-agent:
	mkdir -p $(AGENT_BIN_DIR)
	wget -q https://github.com/prometheus/prometheus/releases/download/v$(PROM_AGENT_VERSION)/$(PROM_AGENT_TARBALL) -O /tmp/$(PROM_AGENT_TARBALL)
	tar -C $(AGENT_BIN_DIR) --strip-components=1 -xzf /tmp/$(PROM_AGENT_TARBALL) prometheus-$(PROM_AGENT_VERSION).linux-amd64/prometheus
	rm -f /tmp/$(PROM_AGENT_TARBALL)

# Runs Prometheus with --enable-feature=agent: scrape and forward only,
# no local query API or long-term storage (see prometheus-agent.yaml for
# scrape targets).
run-prometheus-agent:
	mkdir -p $(PROM_AGENT_DATA_DIR)
	nohup $(PROM_AGENT_BIN) --config.file=$(PROM_AGENT_CONFIG) --agent --storage.agent.path=$(PROM_AGENT_DATA_DIR) > $(PROM_AGENT_LOG) 2>&1 & echo $$! > $(PROM_AGENT_PID)

stop-prometheus-agent:
	@if [ -f $(PROM_AGENT_PID) ]; then \
		xargs -r kill < $(PROM_AGENT_PID); \
		rm -f $(PROM_AGENT_PID); \
		echo "stopped prometheus-agent"; \
	else \
		echo "no $(PROM_AGENT_PID) file found, nothing to stop"; \
	fi

# Downloads the vmutils tarball and pulls out vmagent-prod (renamed to
# vmagent for consistency with the other AGENT_BIN names above). Safe to
# re-run on the same instance.
init-vmagent:
	mkdir -p $(AGENT_BIN_DIR)
	wget -q https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v$(VMAGENT_VERSION)/$(VMAGENT_TARBALL) -O /tmp/$(VMAGENT_TARBALL)
	tar -C $(AGENT_BIN_DIR) -xzf /tmp/$(VMAGENT_TARBALL) vmagent-prod
	mv $(AGENT_BIN_DIR)/vmagent-prod $(VMAGENT_BIN)
	rm -f /tmp/$(VMAGENT_TARBALL)

# Runs vmagent scraping the same targets as prometheus-agent (see
# vmagent.yaml) and remote-writing to the same backend, so the two agents'
# overhead can be compared side by side. -remoteWrite.tmpDataPath is
# vmagent's on-disk write-ahead queue for when the remote endpoint is slow
# or unreachable - the rough equivalent of --storage.agent.path above.
run-vmagent:
	mkdir -p $(VMAGENT_DATA_DIR)
	nohup $(VMAGENT_BIN) \
		-promscrape.config=$(VMAGENT_CONFIG) \
		-remoteWrite.url=$(VMAGENT_REMOTE_WRITE_URL) \
		-remoteWrite.tmpDataPath=$(VMAGENT_DATA_DIR) \
		-remoteWrite.maxRowsPerBlock=10000 \
		-remoteWrite.flushInterval=1s \
		--remoteWrite.forceVMProto \
		> $(VMAGENT_LOG) 2>&1 & echo $$! > $(VMAGENT_PID)

stop-vmagent:
	@if [ -f $(VMAGENT_PID) ]; then \
		xargs -r kill < $(VMAGENT_PID); \
		rm -f $(VMAGENT_PID); \
		echo "stopped vmagent"; \
	else \
		echo "no $(VMAGENT_PID) file found, nothing to stop"; \
	fi
