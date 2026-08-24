MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT_DIR      := $(abspath $(MAKEFILE_DIR)/..)
BIN_DIR       := $(ROOT_DIR)/bin
LOG_DIR       := $(BIN_DIR)/logs

# Each running instance of an app listens on its own port in this range,
# so 100 instances of the same app can run side by side on one host.
PORT_START ?= 8001
PORT_END   ?= 8100

# Collector otel-app pushes OTLP metrics to (see deployment/docker/agent's
# init-otel-agent/run-otel-agent).
OTEL_COLLECTOR_ENDPOINT ?= http://10.140.15.200:4318

# otel-app's metric export cadence, in milliseconds (OTEL_METRIC_EXPORT_INTERVAL,
# read natively by the OTel SDK's periodic reader - see cmd/otel-app/main.go).
# Matches prometheus-agent.yaml's scrape_interval (30s) for a fair comparison.
OTEL_METRIC_EXPORT_INTERVAL ?= 10000

OTEL_EXPORTER_OTLP_METRICS_COMPRESSION ?= gzip

# This host's own internal IP, used to give each otel-app instance a
# service.instance.id resource attribute of "<ip>:<port>" - both
# Prometheus's OTLP receiver and the Collector's remote-write exporter
# convert that attribute into the metric's "instance" label, matching the
# instance="<ip>:<port>" label Prometheus assigns automatically to scraped
# targets like prometheus-app. Auto-detected via hostname -I, which is
# only correct if the host has a single network interface (true for every
# instance in this project); override if that's wrong for yours.
HOST_IP ?= $(shell hostname -I | awk '{print $$1}')

# Hosts run-client fires requests at - 2 otel-app instances, 2
# prometheus-app instances and 1 uninstrumented-app instance, each listening
# on $(PORT_START)-$(PORT_END) (100 ports), for 500 targets total.
CLIENT_HOSTS ?= 10.140.15.196 10.140.15.197 10.140.15.194 10.140.15.195 10.140.15.203

# The "-1" host of each app (matches the host_name labels in
# deployment/docker/backend/prometheus.yaml), used by collect-profiles below.
OTEL_APP_1_HOST           ?= 35.234.13.172
PROMETHEUS_APP_1_HOST     ?= 34.81.241.81
UNINSTRUMENTED_APP_1_HOST ?= 34.81.251.210

# Single port (one specific instance out of the $(PORT_START)-$(PORT_END)
# range) that collect-profiles pulls from on each host.
PROFILE_PORT    ?= 8001
PROFILE_SECONDS ?= 30
PROFILE_DIR     ?= $(BIN_DIR)/profiles

# The two storage-backend hosts from the OTLP-vs-remote-write ingestion
# comparison (each running Prometheus as a full backend - see
# deployment/docker/backend's run-prometheus-server - one fed by otel-agent
# via OTLP, the other by prometheus-agent/vmagent via remote-write), used
# by collect-backend-profiles below. Prometheus exposes net/http/pprof on
# its own web port (9090) unconditionally, no extra flag needed.
PROMETHEUS_STORAGE_HOST ?= 35.194.243.41
OTEL_STORAGE_HOST       ?= 104.199.176.233
BACKEND_PROFILE_PORT    ?= 9090

.PHONY: all otel-app prometheus-app uninstrumented-app client clean tar \
	run-otel-app run-prometheus-app run-uninstrumented-app run-client \
	stop-otel-app stop-prometheus-app stop-uninstrumented-app stop-client stop \
	collect-profiles collect-backend-profiles

all: otel-app prometheus-app uninstrumented-app client

otel-app:
	go build -C $(ROOT_DIR) -o $(BIN_DIR)/otel-app ./cmd/otel-app

prometheus-app:
	go build -C $(ROOT_DIR) -o $(BIN_DIR)/prometheus-app ./cmd/prometheus-app

uninstrumented-app:
	go build -C $(ROOT_DIR) -o $(BIN_DIR)/uninstrumented-app ./cmd/uninstrumented-app

client:
	go build -C $(ROOT_DIR) -o $(BIN_DIR)/client ./cmd/client

clean:
	rm -rf $(BIN_DIR)

# Tars each binary already present in $(BIN_DIR) into its own tar.gz next
# to it (e.g. bin/otel-app -> bin/otel-app.tar.gz).
tar:
	@for bin in $(BIN_DIR)/*; do \
		[ -f "$$bin" ] || continue; \
		case "$$bin" in *.tar.gz) continue ;; esac; \
		name=$$(basename "$$bin"); \
		tar -C $(BIN_DIR) -czf $(BIN_DIR)/$$name.tar.gz $$name; \
		echo "packaged $(BIN_DIR)/$$name.tar.gz"; \
	done

# run-* targets launch one instance per port in $(PORT_START)-$(PORT_END)
# (100 by default), each backgrounded and logging to its own file under
# $(LOG_DIR); PIDs are recorded so the matching stop-* target can kill
# exactly this batch.
run-otel-app: otel-app
	@mkdir -p $(LOG_DIR)
	@: > $(LOG_DIR)/otel-app.pids
	@for port in $$(seq $(PORT_START) $(PORT_END)); do \
		LISTEN_ADDR=":$$port" OTEL_EXPORTER_OTLP_ENDPOINT=$(OTEL_COLLECTOR_ENDPOINT) OTEL_EXPORTER_OTLP_METRICS_COMPRESSION=$(OTEL_EXPORTER_OTLP_METRICS_COMPRESSION) OTEL_METRIC_EXPORT_INTERVAL=$(OTEL_METRIC_EXPORT_INTERVAL) OTEL_RESOURCE_ATTRIBUTES="service.instance.id=$(HOST_IP):$$port" $(BIN_DIR)/otel-app > $(LOG_DIR)/otel-app-$$port.log 2>&1 < /dev/null & \
		echo $$! >> $(LOG_DIR)/otel-app.pids; \
	done
	@echo "started $$(wc -l < $(LOG_DIR)/otel-app.pids) otel-app instances on ports $(PORT_START)-$(PORT_END) (logs: $(LOG_DIR), pids: $(LOG_DIR)/otel-app.pids)"

run-prometheus-app: prometheus-app
	@mkdir -p $(LOG_DIR)
	@: > $(LOG_DIR)/prometheus-app.pids
	@for port in $$(seq $(PORT_START) $(PORT_END)); do \
		LISTEN_ADDR=":$$port" $(BIN_DIR)/prometheus-app > $(LOG_DIR)/prometheus-app-$$port.log 2>&1 < /dev/null & \
		echo $$! >> $(LOG_DIR)/prometheus-app.pids; \
	done
	@echo "started $$(wc -l < $(LOG_DIR)/prometheus-app.pids) prometheus-app instances on ports $(PORT_START)-$(PORT_END) (logs: $(LOG_DIR), pids: $(LOG_DIR)/prometheus-app.pids)"

run-uninstrumented-app: uninstrumented-app
	@mkdir -p $(LOG_DIR)
	@: > $(LOG_DIR)/uninstrumented-app.pids
	@for port in $$(seq $(PORT_START) $(PORT_END)); do \
		LISTEN_ADDR=":$$port" $(BIN_DIR)/uninstrumented-app > $(LOG_DIR)/uninstrumented-app-$$port.log 2>&1 < /dev/null & \
		echo $$! >> $(LOG_DIR)/uninstrumented-app.pids; \
	done
	@echo "started $$(wc -l < $(LOG_DIR)/uninstrumented-app.pids) uninstrumented-app instances on ports $(PORT_START)-$(PORT_END) (logs: $(LOG_DIR), pids: $(LOG_DIR)/uninstrumented-app.pids)"

# Runs a single client process (not one per port, unlike the run-*-app
# targets above) whose CLIENT_TARGETS spans every port on every host in
# $(CLIENT_HOSTS), so one process load-tests all of them.
run-client: client
	@mkdir -p $(LOG_DIR)
	@targets=""; \
	for host in $(CLIENT_HOSTS); do \
		for port in $$(seq $(PORT_START) $(PORT_END)); do \
			targets="$$targets,http://$$host:$$port"; \
		done; \
	done; \
	targets="$${targets#,}"; \
	CLIENT_TARGETS="$$targets" nohup $(BIN_DIR)/client > $(LOG_DIR)/client.log 2>&1 & \
	echo $$! > $(LOG_DIR)/client.pid; \
	echo "started client (1 process) against $$(echo "$$targets" | tr ',' '\n' | wc -l) targets (log: $(LOG_DIR)/client.log, pid: $(LOG_DIR)/client.pid)"

# Pulls a heap profile then a CPU profile (see cmd/*/main.go's
# /debug/pprof/* handlers) from one instance - port $(PROFILE_PORT), the
# same port on every host - of otel-app-1, prometheus-app-1 and
# uninstrumented-app-1. All three hosts are fetched in parallel (the trailing
# "&" on each host's subshell, followed by "wait"), so every app is caught in
# the same window of wall-clock time; that only matters relative to the other
# apps, not within a single host, so heap and profile are still fetched
# sequentially per host - heap first, since /debug/pprof/profile blocks for
# $(PROFILE_SECONDS)s and fetching it first would skew the heap snapshot
# towards "after 30s of CPU profiling" rather than "at the moment requested".
collect-profiles:
	@mkdir -p $(PROFILE_DIR)
	@ts=$$(date +%Y%m%d-%H%M%S); \
	for pair in "otel-app-1:$(OTEL_APP_1_HOST)" "prometheus-app-1:$(PROMETHEUS_APP_1_HOST)" "uninstrumented-app-1:$(UNINSTRUMENTED_APP_1_HOST)"; do \
		name=$${pair%%:*}; host=$${pair#*:}; \
		( curl -sf "http://$$host:$(PROFILE_PORT)/debug/pprof/heap" -o "$(PROFILE_DIR)/$$name-$$ts-heap.pprof" \
		  && curl -sf "http://$$host:$(PROFILE_PORT)/debug/pprof/profile?seconds=$(PROFILE_SECONDS)" -o "$(PROFILE_DIR)/$$name-$$ts-cpu.pprof" \
		  && echo "done: $$name ($$host:$(PROFILE_PORT))" \
		  || echo "FAILED: $$name ($$host:$(PROFILE_PORT))" ) & \
	done; \
	wait; \
	echo "profiles written to $(PROFILE_DIR)"

# Same idea as collect-profiles above, but for the two storage-backend
# hosts instead of the app hosts: pulls heap then CPU pprof (heap first,
# since /debug/pprof/profile blocks for $(PROFILE_SECONDS)s) from both in
# parallel, so their resource usage under OTLP vs. remote-write ingestion
# can be compared for the same window of wall-clock time.
collect-backend-profiles:
	@mkdir -p $(PROFILE_DIR)
	@ts=$$(date +%Y%m%d-%H%M%S); \
	for pair in "prometheus-storage:$(PROMETHEUS_STORAGE_HOST)" "otel-storage:$(OTEL_STORAGE_HOST)"; do \
		name=$${pair%%:*}; host=$${pair#*:}; \
		( curl -sf "http://$$host:$(BACKEND_PROFILE_PORT)/debug/pprof/heap" -o "$(PROFILE_DIR)/$$name-$$ts-heap.pprof" \
		  && curl -sf "http://$$host:$(BACKEND_PROFILE_PORT)/debug/pprof/profile?seconds=$(PROFILE_SECONDS)" -o "$(PROFILE_DIR)/$$name-$$ts-cpu.pprof" \
		  && echo "done: $$name ($$host:$(BACKEND_PROFILE_PORT))" \
		  || echo "FAILED: $$name ($$host:$(BACKEND_PROFILE_PORT))" ) & \
	done; \
	wait; \
	echo "profiles written to $(PROFILE_DIR)"

stop-otel-app:
	@if [ -f $(LOG_DIR)/otel-app.pids ]; then \
		xargs -r kill < $(LOG_DIR)/otel-app.pids; \
		rm -f $(LOG_DIR)/otel-app.pids; \
		echo "stopped otel-app instances"; \
	else \
		echo "no otel-app.pids file found, nothing to stop"; \
	fi

stop-prometheus-app:
	@if [ -f $(LOG_DIR)/prometheus-app.pids ]; then \
		xargs -r kill < $(LOG_DIR)/prometheus-app.pids; \
		rm -f $(LOG_DIR)/prometheus-app.pids; \
		echo "stopped prometheus-app instances"; \
	else \
		echo "no prometheus-app.pids file found, nothing to stop"; \
	fi

stop-uninstrumented-app:
	@if [ -f $(LOG_DIR)/uninstrumented-app.pids ]; then \
		xargs -r kill < $(LOG_DIR)/uninstrumented-app.pids; \
		rm -f $(LOG_DIR)/uninstrumented-app.pids; \
		echo "stopped uninstrumented-app instances"; \
	else \
		echo "no uninstrumented-app.pids file found, nothing to stop"; \
	fi

stop-client:
	@if [ -f $(LOG_DIR)/client.pid ]; then \
		xargs -r kill < $(LOG_DIR)/client.pid; \
		rm -f $(LOG_DIR)/client.pid; \
		echo "stopped client"; \
	else \
		echo "no client.pid file found, nothing to stop"; \
	fi

stop: stop-otel-app stop-prometheus-app stop-uninstrumented-app stop-client
