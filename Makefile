.PHONY: build build-no-opus clean

build:
	go mod download
	go build -tags libopus -o telephony-gateway-poc ./cmd/gateway

build-no-opus:
	go mod download
	go build -o telephony-gateway-poc ./cmd/gateway

clean:
	rm -f telephony-gateway-poc
