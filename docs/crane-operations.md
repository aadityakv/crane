# Crane operations runbook

This document describes how to install, configure, run, drive, and verify the
supported Crane stream-processing runtime. Crane runs inside the same node
process as SWIM membership and fixed-membership Raft; there is no separate
Crane daemon, no dynamic port allocation, and no plugin loading. The legacy
`src/` tree (file system and grep exercises) is reference-only and is not part
of the supported runtime.

## Installation and toolchain

- Go 1.26 exactly (`go.mod` pins the language version; the verification gate
  records `go version`).
- `make build` produces `bin/cs425-node` (the node), `bin/cs425-cluster`
  (local cluster launcher), and `bin/cs425-crane` (the client CLI).
- Optional static analysis: `go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...`.

## Configuration

Every node loads one strict JSON file (`-config`). Unknown fields, trailing
JSON, missing fields, and out-of-range values fail validation before any
listener binds or any storage is touched. The node fields are documented in
the README; the Crane section is:

| Field | Meaning |
| --- | --- |
| `crane.worker_slots` | Task slots this worker offers the cluster (1..256). Placement never exceeds the cluster-wide slot total. |
| `crane.worker_control_timeout` | Per-request timeout for +5 worker-control and +6 public-control exchanges. |
| `crane.tuple_retry_interval` | Resend interval for unacknowledged +7 tuple deliveries. |
| `crane.tuple_completion_retry_interval` | Resend interval for completion acknowledgments awaiting downstream durability. |
| `crane.failure_grace_period` | How long a worker must be continuously Dead/Left and unreachable before its tasks are reassigned. Suspect alone never reassigns. |
| `crane.max_worker_store_bytes` | Durable budget for the worker's write-ahead log plus snapshots; exhaustion fails closed with a retryable capacity error. |
| `crane.consensus_fingerprint` | Hex SHA-256 of the compiled consensus contract (schemas, limits, wire IDs, operator registry, worker-store format). Startup rejects a mismatch before construction; peers with a different fingerprint refuse the Raft and worker handshakes before any RPC. |

`examples/config/node-1.json` shows every field with the working values for
the current commit. The fingerprint is compiled into the binary
(`config.DefaultCraneConfig().ConsensusFingerprint`); a configured value that
does not match is reported at startup with both values, and the operational
fields default when omitted while the fingerprint never does.

## Ports

All ports derive from `base_port` through the typed service registry; nothing
is parsed from hostnames or node IDs.

| Offset | Service | Transport | Role in Crane |
| ---: | --- | --- | --- |
| +0 | `swim-ping` | UDP | membership probes |
| +1 | `swim-ack` | UDP | membership acknowledgments |
| +2 | `swim-snapshot` | TCP | membership join/snapshot |
| +3 | `file-rpc` | TCP | legacy, reference-only |
| +4 | `grep-rpc` | TCP | legacy, reference-only |
| +5 | `crane-worker` | TCP | authenticated coordinator→worker control sessions, worker→worker result transfer and repair |
| +6 | `topology-control` | TCP | public client API: submit, cancel, status, result paging; non-leaders answer with a checked redirect |
| +7 | `crane-tuple-ack` | UDP | bounded 1,200-byte authenticated tuple deliveries and ACK/NACK |
| +8 | `raft-rpc` | TCP | fixed-membership Raft among the configured voters |

## Running

Local cluster, exactly as in the README:

```sh
make build
umask 077
head -c 32 /dev/urandom > local.secret && chmod 600 local.secret
./bin/cs425-cluster -nodes 3 -base-port 8000 -data-root ./data/local \
  -secret-file ./local.secret -node-binary ./bin/cs425-node
```

Multi-host: generate one configuration per host with the same `cluster_id`,
`cluster_secret_file` contents, `raft_voters` map, and `crane` section; set
`advertise_host` to the routable address; start the introducer first, then the
remaining nodes with `./bin/cs425-node -config <file>`. `tools/rsync_exclude.txt`
excludes local state when syncing the tree to hosts.

Voters run Raft, the coordinator (when leading), the worker services, and the
public control service. Nonvoters run the worker services and a control
service that redirects every request to the voters; they own no Raft storage.

## Client CLI

```sh
./bin/cs425-crane example-topology > topology.json
./bin/cs425-crane submit  -config examples/config/node-1.json -state ./client.state -topology topology.json
./bin/cs425-crane status  -config examples/config/node-1.json -state ./client.state -job <32 hex>
./bin/cs425-crane results -config examples/config/node-1.json -state ./client.state -job <32 hex> [-page-bytes N]
./bin/cs425-crane cancel  -config examples/config/node-1.json -state ./client.state -job <32 hex> -expected-revision <job-control revision from status>
```

Every network subcommand also accepts `-attempts N` (complete request
attempts, 1 through 1024, default 8), `-backoff DURATION` (pause between
attempts, default 200ms), and `-timeout DURATION` (per-exchange timeout
override; zero keeps the configured `worker_control_timeout`). The submit
and cancel outputs carry the durable job-control revision the client
validated in the response.

`-state` names an owner-only file that holds the durable client identity:
the client ID, the next request sequence, and any pending mutation. A
sequence is reserved and persisted before the request is sent, so a crash
never reuses a sequence; a restarted client resumes the pending request with
the exact same bytes under a fresh request ID and receives the replicated
cached result. The client validates every submit and cancel response
against the exact durable revision the command must have produced (a
first submit is revision 1; a cancel is the expected revision plus one)
before reporting success. The client follows checked leader redirects to the
static voter set only, retries transient errors (`Starting`,
`CapacityExhausted`, `ResultUnavailable`) within a bounded attempt budget,
and refuses to continue with a forfeited identity (`StaleRequest`,
`SkippedRequest`, `IdentityReuse`) so state is never silently reused.

Topology documents are validated against the immutable operator registry:

| Operator | Role | Settings |
| --- | --- | --- |
| `range` | source | `start`, `end_exclusive` (int64); deterministic, partitioned by parallelism |
| `even` | transform | none; passes even values |
| `less_than` | transform | `threshold` (int64) |
| `multiply` | transform | `factor` (int64) |
| `collect` | sink | none; produces the job's result records |

## Guarantees and recovery

- **Exactly-once results.** Every delivery carries a canonical tuple identity;
  workers keep durable custody (`Received` → `Processed` → `Completed`) in an
  identity-bound, checksummed write-ahead log, and duplicates are answered
  from durable state. Result records are replicated to two current replicas
  before a checkpoint may cover them; a job seals only when both replicas'
  inventories agree.
- **Leader loss.** Leadership, assignments, checkpoints, and manifests live in
  the Raft state machine. A new leader barriers, fences every worker to a new
  coordinator epoch, re-reads worker status, re-installs the exact committed
  scheduling state, repairs result copies, and only then reopens admission.
  Stale coordinators cannot mutate anything after the fence.
- **Worker crash with store preserved.** The process recovers its log and
  snapshots before reporting Ready; its admission gate stays closed until the
  leader re-installs Running; retained custody under a superseded epoch or
  assignment revision is re-adopted when the assignment is otherwise
  unchanged.
- **Worker store loss.** The node restarts with a new worker epoch. Its old
  attempts are fenced (stale deliveries are NACKed without custody), tasks are
  reassigned after `failure_grace_period`, and any result copies it held are
  re-established on the new replica pair through authenticated bilateral
  repair grants (destination first, every chunk bound to the repair ID and
  instruction digest).
- **Checkpoints and replay.** Source watermarks advance only through committed
  checkpoint notices; on failure, replay resumes strictly above the committed
  watermark and tombstones answer late duplicates.
- **Capacity.** Bounded everything: 64 active jobs, 256 retained jobs, 256
  result manifests per job, 64 MiB of result records per job, 1,200-byte tuple
  frames, 1 MiB control frames, and the configured worker-store budget. When
  a bound is reached the system answers a typed retryable capacity error and
  never accepts work it cannot store.
- **Shared-secret limitation.** All authentication is one cluster-wide HMAC
  secret; any holder of the secret is a fully trusted member. There is no
  per-node identity beyond the configured membership and IP/DNS checks.
- **Migration.** Replicated snapshots are schema-versioned; an empty legacy
  snapshot upgrades in place, and a nonempty legacy snapshot is rejected
  rather than reinterpreted. Legacy schema-v1 worker snapshots remain readable
  and new worker snapshots are written as schema v2. Two
  binaries with different consensus fingerprints never join the same cluster.

## Verification

```sh
make test              # ordinary suites (add -short locally to bound the two slowest proofs)
make race              # race detector over the tree
make vet staticcheck   # static analysis
make sim               # deterministic four-process simulation with oracle
make integration       # SWIM/Raft real-process suites
make crane-integration # real four-process Crane failover proof (build-tagged seam)
make verify            # all of the above
```

The deterministic simulation and the real-process proof are the reference for
the failure scenarios above; the recorded verification evidence for each
release is kept outside the repository.
