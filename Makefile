BIN      := bin/regcachectl
PKG      := github.com/mwiget/regcachectl
GOFLAGS  := -trimpath
LDFLAGS  := -s -w

.PHONY: build install test smoke fmt vet tidy clean

build:
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN) .

install: build
	install -D -m 0755 $(BIN) $(HOME)/.local/bin/regcachectl
	@echo "installed -> $(HOME)/.local/bin/regcachectl"

test:
	go test ./...

# smoke: unit tests + the built binary answers its read-only commands
# without a runtime mutation (print-registries / version / help).
smoke: build test
	@echo "== smoke: print-registries =="
	@$(BIN) print-registries | grep -q 'repo.f5.com' || { echo "print-registries broken"; exit 1; }
	@$(BIN) version | grep -q regcachectl || { echo "version broken"; exit 1; }
	@echo "smoke OK"

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin
