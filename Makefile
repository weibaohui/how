# HOW — transparent reverse HTTP proxy over WebSockets.
#
# Common targets:
#   make release      build the release binaries (how-server, how-client, how-test-api)
#   make build        debug build
#   make test         Rust integration tests (sequential)
#   make e2e          shell end-to-end test (curl)
#   make run-server   build & run the server with config.server.example.cfg
#   make run-client   build & run the client with config.client.example.cfg
#   make clean        cargo clean

CARGO ?= cargo

.PHONY: all release build test e2e clean run-server run-client

all: release

release:
	$(CARGO) build --release
	@echo
	@echo "==> release binaries:"
	@for b in how-server how-client how-test-api; do \
		p=target/release/$$b; \
		[ -f "$$p" ] && printf "    %s\n" "$$p"; \
	done

build:
	$(CARGO) build

test:
	$(CARGO) test --test e2e -- --test-threads=1

e2e:
	bash scripts/e2e.sh

clean:
	$(CARGO) clean

run-server: release
	./target/release/how-server -config config.server.example.cfg

run-client: release
	./target/release/how-client -config config.client.example.cfg
