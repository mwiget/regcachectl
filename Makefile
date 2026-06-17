BIN        := bin/regcachectl
PKG        := github.com/mwiget/regcachectl
GOFLAGS    := -trimpath
LDFLAGS    := -s -w
BLOBCACHE_IMAGE := regcache-blobcache:latest

.PHONY: build install test smoke fmt vet tidy clean blobcache-image

build:
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN) .

install: build blobcache-image
	install -D -m 0755 $(BIN) $(HOME)/.local/bin/regcachectl
	@echo "installed -> $(HOME)/.local/bin/regcachectl"

# blobcache-image builds the credential-free repo.f5.com blob-cache image from a
# static linux binary (CGO off → runs on distroless/static).
blobcache-image:
	CGO_ENABLED=0 GOOS=linux go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/regcachectl-linux .
	docker build -t $(BLOBCACHE_IMAGE) .
	@echo "built $(BLOBCACHE_IMAGE)"

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
