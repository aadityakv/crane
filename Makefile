.PHONY: build test race integration vet staticcheck fmt-check verify

build:
	go build -o bin/cs425-node ./cmd/node
	go build -o bin/cs425-cluster ./cmd/cluster

test:
	go test ./...

race:
	go test -race ./...

integration:
	go test -tags=integration ./integration -count=1 -v

vet:
	go vet ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

fmt-check:
	test -z "$$(gofmt -l cmd internal integration)"

verify: fmt-check test race vet staticcheck integration
