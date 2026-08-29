# CS 425 MP3 runtime foundation

This repository contains the modern Go runtime foundation and its SWIM membership service. It runs authenticated nodes locally or on separately addressed machines. The legacy implementation under [`src/`](src/) is a quarantined nested Go module: it is retained for reference only and is not built, imported, or operated by the root module.

## Prerequisites and verification

Install Go 1.26 or newer, then confirm that the selected toolchain is available:

```sh
go version
go env GOMOD
```

The second command must point to this repository's `go.mod`. Run the full local gate before changing or operating the runtime:

```sh
make verify
```

The gate checks formatting, unit tests, race safety, `go vet`, Staticcheck, and the real-process integration test. `make build` produces `bin/cs425-node` and `bin/cs425-cluster`; the other individual targets are `test`, `race`, `integration`, `vet`, `staticcheck`, and `fmt-check`. CI runs the supported checks on current macOS and Linux runners.

## Cluster secret

Every node in one cluster must use the same nonempty secret file. The file is read as the HMAC key and validation rejects any group- or world-readable file. Create one with owner-only permissions before starting a node:

```sh
umask 077
head -c 32 /dev/urandom > local.secret
chmod 600 local.secret
```

Do not commit or place the secret in JSON configuration. The example configuration names `./local.secret` but contains no secret value. Copy the same protected file to each remote host through an approved secure channel.

## Configuration and endpoints

`internal/config.NodeConfig` is the single source of a node's logical identity and all runtime endpoints. A configuration file has these fields:

- `node_id`: stable, nonzero cluster identity.
- `cluster_id`: UUID that separates clusters.
- `bind_host`: local address on which listeners bind; wildcard addresses are allowed here.
- `advertise_host`: routable IP address or DNS name advertised to peers; wildcard addresses are rejected.
- `base_port`: nonzero base used with the typed service registry below.
- `introducer`: a seed node's SWIM snapshot endpoint, used only for initial admission.
- `storage_dir`: non-root directory for the persisted SWIM incarnation state and future subsystem state.
- `cluster_secret_file`: path to the owner-only HMAC key file.
- `raft_voters`: the same static three- or five-member ID/endpoint map in every node configuration.
- `timing`: positive SWIM and replay durations; direct plus indirect probe timeouts may not exceed the probe interval.

Unknown JSON fields, trailing JSON, invalid hosts, unsafe secret permissions, duplicate voters, invalid voter endpoints, and port overflow fail validation before listeners are opened. [`examples/config/node-1.json`](examples/config/node-1.json) is a one-node configuration example; it is part of a three-voter local layout and expects a sibling `local.secret`.

Ports come only from the typed registry, never hostname parsing or node-ID arithmetic. For a `base_port` of `8000`, the complete layout is:

| Offset | Service | Transport | Example port |
| ---: | --- | --- | ---: |
| +0 | `swim-ping` | UDP | 8000 |
| +1 | `swim-ack` | UDP | 8001 |
| +2 | `swim-snapshot` | TCP | 8002 |
| +3 | `file-rpc` | TCP | 8003 |
| +4 | `grep-rpc` | TCP | 8004 |
| +5 | `crane-worker` | TCP | 8005 |
| +6 | `topology-control` | TCP | 8006 |
| +7 | `crane-tuple-ack` | UDP | 8007 |
| +8 | `raft-rpc` | TCP | 8008 |

## Running a node

Build the executables and launch a configured node:

```sh
make build
./bin/cs425-node -config examples/config/node-1.json
```

`-config` is required. For generated local clusters only, the node command accepts `-node-id`, `-bind-host`, `-advertise-host`, `-base-port`, and `-storage-dir` overrides. Cluster identity, seed, voters, timing, and secret location remain file-controlled so a command-line typo cannot create another security or consensus domain.

Startup creates the storage directory with owner-only permissions, loads the last persisted incarnation, binds SWIM's UDP and snapshot TCP listeners, and prints its readiness signal only after the service is ready. The supervisor cancels all services if a required listener or startup invariant fails. Send `SIGINT` or `SIGTERM` for graceful shutdown: the node persists a newer incarnation, announces `Left` for a bounded dissemination interval, then closes listeners and waits for its goroutines. Configuration, authentication-key, storage, listener, and invariant failures exit nonzero; requested graceful shutdown exits zero.

## Local three-node cluster

The launcher generates strict `0600` configuration files under its data root, creates per-node storage directories, and never writes the secret into those files. Its default local bases are 8000, 8100, and 8200 (a stride of 100); node 1 is the initial seed, not a permanent authority.

```sh
make build
umask 077
head -c 32 /dev/urandom > local.secret
chmod 600 local.secret
./bin/cs425-cluster \
  -nodes 3 \
  -base-port 8000 \
  -data-root ./data/local \
  -secret-file ./local.secret \
  -node-binary ./bin/cs425-node
```

`cmd/cluster` starts the seed first, waits for its readiness signal, then launches the remaining nodes with node-ID-prefixed logs. It forwards the first `SIGINT`/`SIGTERM` to the children for graceful leave and waits for them; a second signal escalates shutdown. Five or more local nodes use a five-voter static map; three or four nodes use a three-voter map.

## Remote hosts

Use one config per machine, with a unique `node_id`, separate writable `storage_dir`, the shared protected secret path, and an identical `cluster_id`, timing block, and `raft_voters` map on every node. Set `bind_host` to the interface the local process can bind and `advertise_host` to the routable DNS name or IP peers can contact. The `introducer` must be the configured seed's advertised `swim-snapshot` endpoint (base port `+2`), not its bind-only or wildcard address. Open the registry's UDP and TCP ports as needed by the enabled runtime services.

The introducer admits a joining node and supplies its snapshot; after admission it has no special authority. If the original seed stops, existing members continue probing and disseminating. A new node still needs a configured reachable seed to join. On restart, a node chooses and atomically persists an incarnation higher than both its prior state and the seed's retained value. If its state directory is lost and no seed retains its identity, restore the state or assign a new node ID rather than reusing incarnation zero.

## SWIM behavior and current scope

The SWIM event loop owns membership state. It runs direct probes, then indirect `PING-REQ` probes on timeout; lack of a matching authenticated ACK creates `Suspect`, and the suspicion timer eventually produces `Dead`. A node refutes suspicion by publishing a higher-incarnation `Alive`; `Dead` and `Left` are retained as tombstones to prevent stale resurrection. Membership events and snapshots are copied, bounded views: slow subscribers receive a resynchronization marker instead of blocking SWIM.

Membership failure does not terminate a process, delete data, alter the configured Raft voter set, or reassign Crane work. Authenticating, replay-checking, and schema validation happen before SWIM state mutation. The real-process integration test covers local admission, failure detection, restart with a higher incarnation, and continued operation after seed loss.

This milestone implements the runtime foundation and SWIM membership only. Raft replication and Crane modernization, including their operational behavior, remain later scope; the static voter configuration is present solely as a validated contract for that work.
