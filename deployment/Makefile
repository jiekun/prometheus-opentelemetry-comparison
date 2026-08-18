MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT_DIR      := $(abspath $(MAKEFILE_DIR)/..)
BIN_DIR       := $(ROOT_DIR)/bin
LOG_DIR       := $(BIN_DIR)/logs

# Each running instance of an app listens on its own port in this range,
# so 100 instances of the same app can run side by side on one host.
PORT_START ?= 8001
PORT_END   ?= 8100

.PHONY: all otel-app prometheus-app uninstrumented-app client clean tar \
	run-otel-app run-prometheus-app run-uninstrumented-app \
	stop-otel-app stop-prometheus-app stop-uninstrumented-app stop

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
		LISTEN_ADDR=":$$port" $(BIN_DIR)/otel-app > $(LOG_DIR)/otel-app-$$port.log 2>&1 < /dev/null & \
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

stop: stop-otel-app stop-prometheus-app stop-uninstrumented-app
