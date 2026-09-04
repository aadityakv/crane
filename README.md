# Crane

Crane is a small, exactly-once stream-processing system written in Go with no
external dependencies. A job is a DAG of operators (`range` sources,
`even`/`less_than`/`multiply` transforms, a `collect` sink) partitioned across a
fixed cluster of nodes. Every node runs three cooperating layers in one
process:

- **SWIM membership** — authenticated failure detection with incarnation
  numbers and tombstones; it decides who is alive, never what the cluster does.
- **Fixed-membership Raft** — three or five configured voters replicate the
  job state machine: jobs, worker registrations, assignments, checkpoints, and
  result manifests. Nonvoters run workers only.
- **Crane** — a leader-elected coordinator (fences workers by epoch, reconciles
  assignments, repairs result replicas) and per-node workers with a
  crash-atomic write-ahead store that delivers tuples over authenticated UDP,
  keeps two current copies of every result, and answers duplicates from
  durable state.

Everything speaks over nine fixed ports derived from one `base_port` (see
[Ports](#ports)); all traffic is HMAC-authenticated with a cluster secret and
replay-guarded.

## Run it locally

```sh
make dev
```

This builds the binaries, creates `./local.secret` with owner-only permissions
on first run, starts a three-node cluster with data under `./data/dev`, and
serves a read-only job dashboard at `http://127.0.0.1:8080`. Press Ctrl-C to
stop the whole cluster; re-running `make dev` resumes it (the cluster ID is
persisted under the data root). Submit and inspect jobs from another shell:

```sh
./bin/crane example-topology > topology.json
./bin/crane submit  -config data/dev/configs/node-1.json -state ./client.state -topology topology.json
./bin/crane jobs    -config data/dev/configs/node-1.json
./bin/crane status  -config data/dev/configs/node-1.json -job <job id>
./bin/crane results -config data/dev/configs/node-1.json -job <job id>
```

The client keeps a durable identity file (`-state`): request sequences are
reserved before they are sent, so a crashed client resumes the same request
and receives the replicated cached answer rather than submitting twice.

To run the bundled example topology end-to-end without touching `./data/dev`
(fresh cluster on port base 9000, submit, wait for success, print results,
tear down):

```sh
make demo
```

The manual equivalent of `make dev`:

```sh
make build
./bin/crane-cluster -nodes 3 -base-port 8000 -data-root ./data/dev \
  -secret-file ./local.secret -node-binary ./bin/crane-node -dashboard 127.0.0.1:8080
```

The full operations guide — every configuration field, the port table,
multi-host deployment, the client contract, operator settings, recovery
guarantees, capacity bounds, and migration rules — is in
[`docs/crane-operations.md`](docs/crane-operations.md).

## Guarantees, briefly

- **Exactly-once results.** Tuple deliveries carry canonical identities;
  workers hold durable custody (`Received` → `Processed` → `Completed`) in an
  identity-bound, checksummed WAL and answer duplicates from it. A checkpoint
  can cover a result only once two current replicas hold it; a job seals only
  when both replicas' inventories agree.
- **Leader loss.** All authority lives in Raft. A new leader barriers, fences
  every worker to a fresh coordinator epoch, re-reads worker status,
  re-installs the exact committed assignment state, repairs result copies, and
  only then reopens admission. A superseded coordinator cannot mutate
  anything.
- **Worker crash, with or without its store.** With the store intact, a worker
  recovers its WAL before reporting ready and re-adopts custody; with the
  store lost, it rejoins under a new worker epoch, its old attempts are fenced
  (stale deliveries are refused without custody), and its replica duties are
  re-established through authenticated bilateral repair.
- **Bounded everything.** 1,200-byte tuple frames, 1 MiB control frames, 64
  active / 256 retained jobs, 256 manifests and 64 MiB of results per job, a
  configured worker-store budget; exhaustion is a typed retryable error, never
  silently accepted work.

The one deliberate limitation: authentication is a single shared cluster
secret. Any holder of the secret is a fully trusted member.

## How it is tested

```sh
make test                # unit suites for every package (-short bounds two long proofs)
make race                # the same under the race detector
make sim                 # deterministic in-process four-node simulation with a safety oracle
make integration         # real-process SWIM and Raft suites
make crane-integration   # real four-process Crane failover proof (build-tagged seam)
make verify              # all of the above plus vet, staticcheck, formatting
```

Three layers of evidence, each stronger than the last:

1. **Unit tests** pin codecs (golden byte vectors, fuzz seeds, allocation
   bounds), the replicated state machine (command dedup, snapshot validation),
   the worker store (crash-atomic WAL, a 21-phase injected fault matrix), and
   the coordinator's reconciliation order.
2. **Deterministic simulation** (`internal/crane/sim`) runs four real node
   runtimes over a simulated network and virtual clock through scripted
   failures (leader loss, packet loss and duplication, exact crash points,
   false suspicion, partition and heal, store loss, sink replica loss and
   repair, full cluster restart) plus seeded randomized schedules, with an
   oracle checking safety invariants after every step.
3. **Real-process integration** (`integration/`) launches four `crane-node`
   processes and drives the same failures with exact durable-boundary crash
   points through a build-tagged hook that production binaries do not contain.

## Configuration and endpoints

`internal/config.NodeConfig` is the single source of a node's identity and
endpoints. A configuration file has these fields:

- `node_id`: stable, nonzero cluster identity.
- `cluster_id`: UUID that separates clusters.
- `bind_host`: local address on which listeners bind; wildcards allowed.
- `advertise_host`: routable IP or DNS name advertised to peers.
- `base_port`: nonzero base for the typed service registry below.
- `introducer`: a seed node's SWIM snapshot endpoint, used only for admission.
- `storage_dir`: owner-only directory for SWIM incarnation, Raft, and worker
  state.
- `cluster_secret_file`: path to the owner-only HMAC key file.
- `raft_voters`: the same three- or five-member ID/endpoint map on every node.
- `timing`: SWIM probe, suspicion, and replay-window durations.
- `crane`: worker slots, control and retry timeouts, failure grace period,
  worker-store budget, and the consensus fingerprint the binary must match.

Unknown fields, trailing JSON, invalid hosts, unsafe secret permissions,
duplicate voters, and port overflow fail validation before anything binds.
`examples/config/node-1.json` is a complete working example.

### Ports

| Offset | Service | Transport | Purpose |
| ---: | --- | --- | --- |
| +0 | `swim-ping` | UDP | membership probes |
| +1 | `swim-ack` | UDP | membership acknowledgments |
| +2 | `swim-snapshot` | TCP | membership join and snapshots |
| +3 | `file-rpc` | TCP | reserved, unused |
| +4 | `grep-rpc` | TCP | reserved, unused |
| +5 | `crane-worker` | TCP | coordinator→worker control, worker→worker result transfer |
| +6 | `topology-control` | TCP | public client API; non-leaders answer with a checked redirect |
| +7 | `crane-tuple-ack` | UDP | tuple deliveries and ACK/NACK |
| +8 | `raft-rpc` | TCP | Raft among the configured voters |

## Cluster secret

Every node in a cluster uses the same secret file containing at least 32 raw
bytes. Validation rejects group- or world-readable files and short keys.
Create it once with owner-only permissions (`umask 077; head -c 32
/dev/urandom > local.secret; chmod 600 local.secret`) and copy it to each host
over a secure channel. Never place the secret in JSON configuration.

## Membership and consensus behavior

**SWIM.** Direct probes, then indirect `PING-REQ` probes on timeout; a missing
authenticated ACK creates `Suspect`, and the suspicion timer produces `Dead`.
A node refutes suspicion with a higher-incarnation `Alive`; `Dead` and `Left`
are retained as tombstones so a stale incarnation can never resurrect. On
restart a node persists an incarnation higher than both its prior state and
the seed's retained value. Membership failure never deletes data, changes the
Raft voter set, or by itself reassigns Crane work; Crane reassigns only after
a worker has been continuously `Dead`/`Left` and unreachable for the
configured grace period.

**Raft.** Every configuration carries the same fixed `raft_voters` map. A
process runs Raft only if its own `node_id` is in that map; a majority (two of
three, three of five) is required to elect and commit. Each voter keeps
identity-bound state under `<storage_dir>/raft` (`identity`, `wal`, `lock`,
`snapshot`); preserve that directory and the SWIM incarnation file across
restarts. Peers whose compiled consensus fingerprint differs are refused at
the handshake, before any RPC.

## Remote hosts

One configuration per machine with a unique `node_id`, its own `storage_dir`,
the shared secret path, and identical `cluster_id`, `timing`, `raft_voters`,
and `crane` sections. Set `advertise_host` to the routable address, start the
introducer first, then the remaining nodes:

```sh
umask 077
storage_dir=/var/lib/crane/node-1
mkdir -p "$storage_dir" && chmod 700 "$storage_dir"
test ! -e "$storage_dir/swim.incarnation" || { echo "refusing to overwrite existing SWIM identity state" >&2; exit 1; }
printf '1\n' > "$storage_dir/swim.incarnation" && chmod 600 "$storage_dir/swim.incarnation"
./bin/crane-node -config /etc/crane/node-1.json
```

The introducer only admits and supplies a snapshot; after admission it has no
special role.

## Toolchain

The module targets Go 1.26 and has no external dependencies. `make verify`
runs formatting, unit, race, vet, staticcheck, and the real-process suites.
CI runs the fast gates on every push (Linux and macOS) and the race detector
on Linux; the four-process failover proof runs nightly and on demand, since it
is timing-sensitive on shared runners.
