include deployment/Makefile
include deployment/docker/agent/Makefile
include deployment/docker/backend/Makefile

# Prerequisites on a fresh instance (must happen before this file is
# available, so they stay manual):
#   sudo apt install git make
#   git clone https://github.com/jiekun/prometheus-opentelemetry-comparison.git

GO_VERSION := 1.26.5
GO_TARBALL := go$(GO_VERSION).linux-amd64.tar.gz

NODE_EXPORTER_VERSION := 1.12.1
NODE_EXPORTER_TARBALL := node_exporter-$(NODE_EXPORTER_VERSION).linux-amd64.tar.gz
NODE_EXPORTER_BIN      := $(BIN_DIR)/node_exporter
NODE_EXPORTER_LOG      := $(LOG_DIR)/node_exporter.log
NODE_EXPORTER_PID      := $(LOG_DIR)/node_exporter.pid

.PHONY: init init-node-exporter run-node-exporter stop-node-exporter

# Installs the Go toolchain used to build the bench binaries and wires
# it onto PATH via ~/.bashrc, then installs and starts node_exporter so
# this host's own metrics are scraped alongside the bench apps. Safe to
# re-run on the same instance.
init: init-node-exporter run-node-exporter
	wget -q https://go.dev/dl/$(GO_TARBALL) -O /tmp/$(GO_TARBALL)
	sudo rm -rf /usr/local/go
	sudo tar -C /usr/local -xzf /tmp/$(GO_TARBALL)
	rm -f /tmp/$(GO_TARBALL)
	grep -qxF 'export PATH=$$PATH:/usr/local/go/bin' ~/.bashrc || echo 'export PATH=$$PATH:/usr/local/go/bin' >> ~/.bashrc
	@echo "Go $(GO_VERSION) installed. Run 'source ~/.bashrc' (or restart your shell) to update PATH."

# Downloads the node_exporter binary used by run-node-exporter. Safe to
# re-run on the same instance.
init-node-exporter:
	mkdir -p $(BIN_DIR)
	wget -q https://github.com/prometheus/node_exporter/releases/download/v$(NODE_EXPORTER_VERSION)/$(NODE_EXPORTER_TARBALL) -O /tmp/$(NODE_EXPORTER_TARBALL)
	tar -C $(BIN_DIR) --strip-components=1 -xzf /tmp/$(NODE_EXPORTER_TARBALL) node_exporter-$(NODE_EXPORTER_VERSION).linux-amd64/node_exporter
	rm -f /tmp/$(NODE_EXPORTER_TARBALL)

run-node-exporter:
	@mkdir -p $(LOG_DIR)
	nohup $(NODE_EXPORTER_BIN) > $(NODE_EXPORTER_LOG) 2>&1 & echo $$! > $(NODE_EXPORTER_PID)

stop-node-exporter:
	@if [ -f $(NODE_EXPORTER_PID) ]; then \
		xargs -r kill < $(NODE_EXPORTER_PID); \
		rm -f $(NODE_EXPORTER_PID); \
		echo "stopped node_exporter"; \
	else \
		echo "no $(NODE_EXPORTER_PID) file found, nothing to stop"; \
	fi
