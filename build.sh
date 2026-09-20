#!/bin/sh
set -eu

# v0.9.1 is intentionally pinned to a dependency set compatible with Go 1.19.
# Do not run `go get -u` here: it would upgrade Pion dependencies and can pull
# releases that require a newer Go toolchain.

ver="$(go version | awk '{print $3}')"
case "$ver" in
  go1.19*|go1.2*|go1.3*|go1.4*|go1.5*|go1.6*|go1.7*|go1.8*|go1.9*) ;;
  *) echo "Unsupported Go toolchain: $ver (need Go >= 1.19)" >&2; exit 1 ;;
esac

if ! pkg-config --exists opus; then
  echo "libopus development files missing. Install: apt install -y pkg-config libopus-dev" >&2
  exit 1
fi

echo "Using $ver"
echo "Downloading pinned Go modules..."
go mod download all

echo "Running tests..."
go test -mod=mod ./...

echo "Building gateway..."
go build -tags libopus -mod=mod -o telephony-gateway-poc ./cmd/gateway
sha256sum telephony-gateway-poc > telephony-gateway-poc.sha256

echo "Build OK: ./telephony-gateway-poc"
