.PHONY: build crane clean-build test race integration crane-integration sim vet staticcheck fmt-check verify dev demo

build: | bin
	go build -o bin/crane-node ./cmd/node
	go build -o bin/crane-cluster ./cmd/cluster
	go build -o bin/crane ./cmd/crane

crane: | bin
	go build -o bin/crane ./cmd/crane

bin:
	mkdir -p $@

clean-build:
	sh scripts/test-clean-build.sh

test:
	go test ./...

race:
	go test -race ./...

integration:
	go test -tags=integration ./integration -count=1 -v

# Real four-process Crane failover proof. Only this target compiles the
# craneintegration seam (durable-boundary events and bounded datagram faults
# driven over one inherited test-owned descriptor); production binaries
# never contain it. The explicit timeout keeps go test's default 10m binary
# budget from panicking mid-scenario (a panic skips t.Cleanup and leaks node
# processes).
crane-integration:
	go test -tags='integration craneintegration' ./integration -run TestCraneLifecycle -count=1 -v -timeout 40m

sim:
	go test ./internal/crane/sim -run TestScripted -count=1
	go test ./internal/crane/sim -run TestRandomized -count=1

dev: build
	./bin/crane-cluster -nodes 3 -base-port 8000 -data-root ./data/dev -secret-file ./local.secret -node-binary ./bin/crane-node -dashboard 127.0.0.1:8080

demo:
	sh scripts/demo.sh

vet:
	go vet ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

fmt-check:
	test -z "$$(gofmt -l cmd internal integration)"

verify: fmt-check test race vet staticcheck integration crane-integration
