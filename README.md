# CS 425 MP3 runtime foundation

This repository contains the modern Go runtime foundation and its SWIM membership service. It runs authenticated nodes locally or on separately addressed machines. The legacy implementation under [`src/`](src/) is a quarantined nested Go module: it is retained for reference only and is not built, imported, or operated by the root module.

## Prerequisites and verification

Install Go 1.26.x for the full pinned verification gate and CI, then confirm that the selected toolchain is available:

```sh
go version
go env GOMOD
```

The second command must point to this repository's `go.mod`. The module's Go version floor remains 1.26. A newer Go release may build and test the project, but the pinned Staticcheck v0.7.0 analyzer is verified only with Go 1.26.x; use that toolchain for `make verify`.

Run the full local gate, including a build from a clean copied tree, before
changing or operating the runtime:

```sh
make clean-build verify
```

The gate checks clean-tree build isolation, formatting, unit tests, race safety, `go vet`, Staticcheck, and the real-process integration test. `make build` produces `bin/cs425-node` and `bin/cs425-cluster`; the other individual targets are `clean-build`, `test`, `race`, `integration`, `vet`, `staticcheck`, and `fmt-check`. CI runs the supported checks on current macOS and Linux runners.

## Cluster secret

Every node in one cluster must use the same secret file containing at least 32 raw bytes (a 256-bit HMAC key). Validation reads the opened regular file, rejects group- or world-readable permissions, and rejects empty or shorter key material. Create it once with owner-only permissions before starting a node:

```sh
umask 077
test ! -e local.secret || { echo "refusing to overwrite local.secret" >&2; exit 1; }
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
- `storage_dir`: non-root directory for persisted SWIM incarnation and fixed-voter Raft state, with space for later Crane/SDFS state.
- `cluster_secret_file`: path to the owner-only HMAC key file.
- `raft_voters`: the same static three- or five-member ID/endpoint map in every node configuration.
- `timing`: positive SWIM and replay durations; direct plus indirect probe timeouts may not exceed the probe interval.

Unknown JSON fields, trailing JSON, invalid hosts, unsafe secret permissions, duplicate voters, invalid voter endpoints, and port overflow fail validation before listeners are opened. [`examples/config/node-1.json`](examples/config/node-1.json) is a one-node configuration example; it is part of a three-voter local layout and expects a sibling `local.secret`.

Ports come only from the typed registry, never hostname parsing or node-ID arithmetic. The modeled registry in [`internal/config/service.go`](internal/config/service.go) is authoritative for service identity, offset, and transport; tests and local-cluster port reservation derive their maximum offset from it. For a `base_port` of `8000`, the complete layout is:

| Offset | Service | Transport | Example port |
| ---: | --- | --- | ---: |
| +0 | `swim-ping` | UDP | 8000 |
| +1 | `swim-ack` | UDP | 8001 |
| +2 | `swim-snapshot` | TCP | 8002 |
| +3 | `file-rpc` | TCP (legacy, reference-only) | 8003 |
| +4 | `grep-rpc` | TCP (legacy, reference-only) | 8004 |
| +5 | `crane-worker` | TCP | 8005 |
| +6 | `topology-control` | TCP | 8006 |
| +7 | `crane-tuple-ack` | UDP | 8007 |
| +8 | `raft-rpc` | TCP | 8008 |

## Running a node

Build the executables, provision the example node's identity state exactly once, and launch it:

```sh
make build
umask 077
state_dir=./data/node-1
mkdir -p "$state_dir"
chmod 700 "$state_dir"
test ! -e "$state_dir/swim.incarnation" || { echo "refusing to overwrite existing SWIM identity state" >&2; exit 1; }
printf '1\n' > "$state_dir/swim.incarnation"
chmod 600 "$state_dir/swim.incarnation"
./bin/cs425-node -config examples/config/node-1.json
```

The secret-creation command in the preceding section must also have created `./local.secret`, the path used by the example config. Writing `swim.incarnation` is an explicit first-run trust/bootstrap ceremony: it establishes the initial nonzero identity generation before any network admission. It deliberately refuses overwriting state. On every later restart, preserve this file; SWIM atomically increments it when required. A corrupted state is rejected. A missing prior state is never silently reset to zero: recovery requires authenticated seed knowledge of the identity, otherwise admission is refused and the operator must restore state or allocate a new node ID.

`-config` is required. For generated local clusters only, the node command accepts `-node-id`, `-bind-host`, `-advertise-host`, `-base-port`, and `-storage-dir` overrides. Cluster identity, seed, voters, timing, and secret location remain file-controlled so a command-line typo cannot create another security or consensus domain.

Startup creates a missing storage directory as `0700`. If the final path already exists, it must be a real directory owned by the current user with exactly `0700` permissions; startup rejects an unsafe path without silently applying `chmod`. The incarnation path must be a non-symlink regular file owned by the current user, grant no group or other permissions, and fit the bounded decimal-state representation; FIFOs, directories, links, oversized files, and permissive files are rejected before reading. After loading identity, every node binds SWIM's UDP and snapshot TCP listeners. A configured Raft voter also recovers its durable Raft state and binds its `raft-rpc` TCP listener at `+8`; a nonvoter does neither. The process prints one readiness signal only after every service constructed for that node is ready. Raft readiness does not wait for a leader or quorum. The supervisor cancels and joins all services if a required listener or startup invariant fails. Send `SIGINT` or `SIGTERM` for graceful shutdown: the node persists a newer SWIM incarnation, announces `Left` for a bounded dissemination interval, then closes listeners and waits for every service goroutine. Configuration, authentication-key, storage, listener, and invariant failures exit nonzero; requested graceful shutdown exits zero.

## Local three-node cluster

The launcher generates strict `0600` configuration files under its data root, creates per-node storage directories and initial nonzero incarnation state, and never writes the secret into those files. Its default local bases are 8000, 8100, and 8200 (a stride of 100); node 1 is the initial seed, not a permanent authority.

```sh
make build
umask 077
test ! -e local.secret || { echo "refusing to overwrite local.secret" >&2; exit 1; }
head -c 32 /dev/urandom > local.secret
chmod 600 local.secret
./bin/cs425-cluster \
  -nodes 3 \
  -base-port 8000 \
  -data-root ./data/local \
  -secret-file ./local.secret \
  -node-binary ./bin/cs425-node
```

`cmd/cluster` starts the seed first, waits for its readiness signal, then launches the remaining nodes with node-ID-prefixed logs. Missing per-node and configuration directories are created as `0700`; unsafe existing final directories are rejected without permission repair. It forwards the first `SIGINT`/`SIGTERM` to the children for graceful leave and waits for them; a second signal escalates shutdown. A child operational failure remains a launcher error even after the user requested shutdown. Five or more local nodes use a five-voter static map; three or four nodes use a three-voter map.

## Fixed-membership Raft behavior

Every node configuration carries the same fixed `raft_voters` map containing exactly three or five stable node IDs and their advertised `raft-rpc` (`+8`) endpoints. A process starts Raft only when its exact local `node_id` appears in that map. SWIM admission, suspicion, failure, recovery, and membership size never add, remove, promote, or replace a Raft voter. In a four-node generated cluster, nodes 1-3 are voters and node 4 is a SWIM-only nonvoter; node 4 does not bind `+8`, open Raft storage, vote, or become a voter when it joins SWIM.

Each voter stores identity-bound consensus state under `<storage_dir>/raft`. The `identity` file binds the store to the configured cluster ID, local node ID, storage format, and complete voter set; `wal` contains committed persistence transactions, `lock` prevents concurrent use, and `snapshot` appears after application snapshotting. Preserve this entire directory and the SWIM incarnation file for a same-identity restart. A voter safely recovers only when its configured identity and fixed voter map still match the durable identity; do not copy another voter's store, delete selected artifacts, or reuse a lost node ID with freshly initialized state.

Raft requires a majority of the configured voters: two of three or three of five. When a majority cannot communicate, the remaining minority cannot elect a usable leader or commit new work. Durable committed state is retained and normal progress resumes after enough same-identity voters return and reconcile. SWIM may report those processes Alive, Suspect, Dead, or Left, but those observations do not change the quorum definition. Changing the voter set requires a future explicitly designed consensus membership protocol; editing live configurations independently is unsafe and unsupported.

The current Raft application is an explicit bootstrap state machine. It restores and snapshots only a versioned empty state and rejects application commands, so no public proposal or debug endpoint is exposed yet. Crane's schema-validated replicated job state, public topology/client control protocol, worker assignment, tuple acknowledgments, deduplication, and failover recovery are the next milestone.

## Remote hosts

Use one config per machine, with a unique `node_id`, separate writable `storage_dir`, the shared protected secret path, and an identical `cluster_id`, timing block, and `raft_voters` map on every node. Set `bind_host` to the interface the local process can bind and `advertise_host` to the routable DNS name or IP peers can contact. The `introducer` must be the configured seed's advertised `swim-snapshot` endpoint (base port `+2`), not its bind-only or wildcard address. Open the registry's UDP and TCP ports as needed by the enabled runtime services.

Before the first remote start, securely create or copy the shared 32-byte-or-longer secret to the exact `cluster_secret_file` path on each host, then provision the configured storage directory once on that host (substitute its exact configured path). This is the same explicit identity-trust ceremony as the standalone example; do not repeat it for restarts:

```sh
umask 077
storage_dir=/var/lib/cs425/node-1
mkdir -p "$storage_dir"
chmod 700 "$storage_dir"
test ! -e "$storage_dir/swim.incarnation" || { echo "refusing to overwrite existing SWIM identity state" >&2; exit 1; }
printf '1\n' > "$storage_dir/swim.incarnation"
chmod 600 "$storage_dir/swim.incarnation"
./bin/cs425-node -config /etc/cs425/node-1.json
```

The introducer admits a joining node and supplies its snapshot; after admission it has no special authority. If the original seed stops, existing members continue probing and disseminating. A new node still needs a configured reachable seed to join. On restart, a node chooses and atomically persists an incarnation higher than both its prior state and the seed's retained value. If its state directory is lost, do not recreate `swim.incarnation` with `1`: recovery requires a seed-retained identity; without it, admission is refused and the operator must restore the state or assign a new node ID rather than reusing incarnation zero.

## SWIM behavior and current scope

The SWIM event loop owns membership state. It runs direct probes, then indirect `PING-REQ` probes on timeout; lack of a matching authenticated ACK creates `Suspect`, and the suspicion timer eventually produces `Dead`. A node refutes suspicion by publishing a higher-incarnation `Alive`; `Dead` and `Left` are retained as tombstones to prevent stale resurrection. If a join acceptance is lost after admission, the client performs one exact idempotent retry without advancing durable incarnation state again. Membership events and snapshots are copied, bounded views: slow subscribers receive a resynchronization marker instead of blocking SWIM. Recovery is scoped to the returned subscription handle: call `Subscription.Snapshot` after its marker; a general `Service.Snapshot` does not resume any subscriber. If membership advances between capture and the owner-confined recovery acknowledgment, `Subscription.Snapshot` returns `swim.ErrSnapshotSuperseded`, leaves that subscription paused, and the caller should retry. If the subscription context removes the handle before acknowledgment, it returns `swim.ErrSubscriptionClosed`. `Service.Stats` exposes only fixed-label, saturating counts for rejected UDP datagrams and transient send failures; it does not retain endpoints, request IDs, payloads, or secrets.

Membership failure does not terminate a process, delete data, alter the configured Raft voter set, or reassign Crane work. Authenticating, replay-checking, and schema validation happen before SWIM state mutation. The real-process integration tests cover local admission, failure detection, restart with a higher incarnation, continued operation after seed loss, and a four-process layout in which only the three fixed voters bind Raft and create durable Raft artifacts.

The runtime foundation, full SWIM membership, fixed-membership Raft replication and persistence, and the Crane stream-processing system (`internal/crane`, documented below) are implemented and verified together; the removed legacy `pkg/topology` bridge is superseded by the validated topology model in `internal/crane/model`.

## Crane stream processing

Crane is the supported stream-processing runtime and runs inside every node
process alongside SWIM and Raft: voters host the replicated coordinator state
and the leader's coordinator, every node hosts the worker services on +5/+7,
and the public client API is served on +6 with checked leader redirects. The
`crane` configuration section, the client CLI (`make crane` builds
`bin/cs425-crane`), the operator registry, the recovery guarantees, and the
verification gates are documented in [`docs/crane-operations.md`](docs/crane-operations.md).

The `src/` directory (file system and grep exercises) is reference-only
legacy code: it is not built into the node, not covered by the Crane
verification gates, and the removed legacy Crane runtime (`src/crane`,
`src/topology`, `src/treejob`, `pkg/topology`) is superseded entirely by
`internal/crane`.
