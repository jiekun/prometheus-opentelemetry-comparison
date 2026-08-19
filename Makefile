include deployment/Makefile
include deployment/docker/agent/Makefile
include deployment/docker/backend/Makefile

# Prerequisites on a fresh instance (must happen before this file is
# available, so they stay manual):
#   sudo apt install git make
#   git clone https://github.com/jiekun/prometheus-opentelemetry-comparison.git

GO_VERSION := 1.26.5
GO_TARBALL := go$(GO_VERSION).linux-amd64.tar.gz

.PHONY: init

# Installs the Go toolchain used to build the bench binaries and wires
# it onto PATH via ~/.bashrc. Safe to re-run on the same instance.
init:
	wget -q https://go.dev/dl/$(GO_TARBALL) -O /tmp/$(GO_TARBALL)
	sudo rm -rf /usr/local/go
	sudo tar -C /usr/local -xzf /tmp/$(GO_TARBALL)
	rm -f /tmp/$(GO_TARBALL)
	grep -qxF 'export PATH=$$PATH:/usr/local/go/bin' ~/.bashrc || echo 'export PATH=$$PATH:/usr/local/go/bin' >> ~/.bashrc
	@echo "Go $(GO_VERSION) installed. Run 'source ~/.bashrc' (or restart your shell) to update PATH."
