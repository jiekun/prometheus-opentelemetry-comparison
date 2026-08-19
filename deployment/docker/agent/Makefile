AGENT_DIR         := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
AGENT_BIN_DIR     := $(abspath $(AGENT_DIR)../../../bin)

OTEL_VERSION      ?= 0.159.0
OTEL_TARBALL      := otelcol_$(OTEL_VERSION)_linux_amd64.tar.gz
OTEL_BIN          := $(AGENT_BIN_DIR)/otelcol
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

.PHONY: init-otel-agent run-otel-agent stop-otel-agent \
	init-prometheus-agent run-prometheus-agent stop-prometheus-agent

# Downloads the otelcol binary used by run-otel-agent. Safe to
# re-run on the same instance.
init-otel-agent:
	mkdir -p $(AGENT_BIN_DIR)
	wget -q https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$(OTEL_VERSION)/$(OTEL_TARBALL) -O /tmp/$(OTEL_TARBALL)
	tar -C $(AGENT_BIN_DIR) -xzf /tmp/$(OTEL_TARBALL) otelcol
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
