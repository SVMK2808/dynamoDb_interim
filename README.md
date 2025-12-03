# DynamoDB1 (Educational Dynamo-style KV Store)

An educational, minimal implementation of a Dynamo-style eventually consistent key-value store in Go. It focuses on the core ideas:

- Consistent hashing ring and preference lists (N replicas)
- Quorum reads/writes (R/W)
- Vector clocks for versioning and conflict resolution
- Anti-entropy via Merkle trees to repair divergence (currently disabled)
- Hinted handoff for durable write availability when replicas are down
- SWIM-style gossip for membership

The code is designed for learning and experimentation rather than production use.

---

## Repository Layout

```
cmd/
  client/           # CLI for basic get/put via coordinator or replica services
  server/           # Node server binary (replica + coordinator + gossip)
internal/
  antientropy/      # Integration tests for convergence
  coordinator/      # Coordinator (quorum logic; anti-entropy disabled)
  gossip/           # SWIM-style membership (simplified)
  hintstore/        # Durable hinted-handoff storage (BoltDB)
  merkle/           # Merkle utilities (hashing, roots)
  replica/          # Replica gRPC service (ReplicaPut/Get/StreamMerkle/TransferItem)
  ring/             # Consistent hash ring & vnode tokens
  storage/          # BoltDB-backed key storage and iteration helpers
  vclock/           # Vector clock operations
proto/
  *.proto           # Protobuf service definitions
  ...pb.go          # Generated stubs (already checked in)
```

---

## Build

Prerequisites:

- Go 1.20+ (the repo may use a newer minor; Go 1.21+ recommended)

Build the server and client binaries:

```bash
# From repo root
go build -o bin/dynamo-server ./cmd/server
go build -o bin/dynamo-client ./cmd/client
```

Run unit tests:

```bash
go test ./...
```

Note: Some integration tests under `internal/antientropy` expect `bin/dynamo-server` and `bin/dynamo-client` to be present and will skip if not found.

---

## Quick start (3-node local cluster)

Open three terminals and start three nodes on localhost:

Terminal A

```bash
./bin/dynamo-server \
  -id nodeA -port 7001 -data ./data-nodeA.db \
  -peers nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003 \
  -vnodes 16 -N 3 -R 2 -W 2
```

Terminal B

```bash
./bin/dynamo-server \
  -id nodeB -port 7002 -data ./data-nodeB.db \
  -peers nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003 \
  -vnodes 16 -N 3 -R 2 -W 2
```

Terminal C

```bash
./bin/dynamo-server \
  -id nodeC -port 7003 -data ./data-nodeC.db \
  -peers nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003 \
  -vnodes 16 -N 3 -R 2 -W 2
```

Write and read through the coordinator (recommended for clients):

```bash
# Put a value (coordinator service on nodeA)
./bin/dynamo-client -addr localhost:7001 -svc coord -cmd put -key user:101 -val Alice -timeout 20

# Get the value (quorum read through coordinator)
./bin/dynamo-client -addr localhost:7001 -svc coord -cmd get -key user:101 -timeout 20
```

You can also contact a replica directly for development and tests:

```bash
./bin/dynamo-client -addr localhost:7002 -svc replica -cmd get -key user:101 -timeout 10
```

---

## Command-line flags (server)

- `-id` string: Unique node ID (e.g., nodeA)
- `-port` int: gRPC listen port
- `-data` string: BoltDB path for canonical data (e.g., ./data-nodeA.db)
- `-peers` string: Comma-separated `nodeID=host:port` list for cluster membership
- `-vnodes` int: Virtual nodes per physical node (for the ring)
- `-N` int: Replication factor (preference list size)
- `-R` int: Read quorum
- `-W` int: Write quorum

Defaults: `N=3`, `R=2`, `W=2`. At startup, the server validates that `1 <= R <= N` and `1 <= W <= N`.

Example (weaker consistency): `-N 3 -R 1 -W 1`.
Example (stronger writes): `-N 3 -R 2 -W 3`.

Hint store path: derived from `-data` by replacing `.db` with `.hints.db` (e.g., `data-nodeA.hints.db`).

---

## Consistency and replication model

- Ring and preference list: Keys are mapped to nodes using a consistent hashing ring with vnodes. The coordinator computes a preference list of size N for each key.
- Quorum writes (W): A write succeeds when W replicas acknowledge. The coordinator writes locally and in parallel to the remaining N-1 replicas.
- Quorum reads (R): A read succeeds when R replicas respond; the coordinator resolves versions using vector clocks.
  - Note: Read repair is disabled to keep the system simple and tunable-consistency focused.

You can also override R/W per request via the coordinator gRPC API (CLI currently uses the defaults configured at startup).
- Vector clocks: Each version carries a vector clock. Merges follow standard Dynamo semantics: dominance overwrites; concurrency creates siblings.

### Failure handling: hinted handoff

When a target replica is unreachable during a write:

- The coordinator persists a “hint” for that target in a durable `HintStore` (BoltDB file separate from canonical data).
- A background delivery worker periodically attempts to deliver hints to their target via ReplicaPut RPCs.
- Delivery is idempotent (vector clock-based). Hints are removed on success; a GC worker prunes stale hints after TTL expiry if needed.

### Anti-entropy: Merkle-based repair (disabled)

Anti-entropy background repair and related RPCs are disabled in this build to keep the project as a simple coordinator + quorum (R/W) + tunable consistency example. The following are no-ops/unimplemented:

- Coordinator anti-entropy worker and merkle comparison
- Replica `StreamMerkle` and `TransferItem` RPCs
- Coordinator read-repair on GET path

---

## Services and RPCs

Protobuf services (already generated to `*_pb.go`):

- Dynamo (replica):
  - `ReplicaPut` (apply/merge item versions)
  - `ReplicaGet` (retrieve siblings)
  - `StreamMerkle` (unimplemented; anti-entropy disabled)
  - `TransferItem` (unimplemented; anti-entropy disabled)
- Coordinator:
  - Quorum `Put`/`Get` are exposed via the CLI client and coordinator gRPC service.

Coordinator-to-replica calls include metadata headers (e.g., `role=coordinator`) so replicas can distinguish internal traffic from direct client calls.

---

## Testing

Run all tests (unit + small integration):

```bash
go test ./...
```

Notes:

- Some tests (e.g., anti-entropy integration) may skip if `bin/dynamo-server` or `bin/dynamo-client` are not available.
- Storage, ring, vclock, merkle, gossip, and hintstore packages include focused unit tests.

---

## Operational notes

- Data files: Each server uses a BoltDB file for canonical data (e.g., `data-nodeA.db`). Hints are stored in a distinct file (e.g., `data-nodeA.hints.db`). Avoid mixing the two.
- Logs: The server logs coordinator decision-making, anti-entropy activity, and hint persistence/delivery.
- Timeouts: Coordinator dial/RPC timeouts are short by default to keep tests snappy; tune them if you see transient failures on slow machines.

---

## Troubleshooting

- “dial failed” errors: Ensure the `-peers` map includes the current node and all destinations with matching ports.
- Quorum timeouts: Increase `-R`/`-W` slack (e.g., set both to 1) for small demos, or increase timeouts in code if your environment is slow.
- Missing binaries in tests: Build `bin/dynamo-server` and `bin/dynamo-client` before running integration tests that spawn processes.
- Stale local data: Delete `./data-node*.db` and `./data-node*.hints.db` if you want a clean slate between runs.

---

## Roadmap / ideas

- Expose metrics endpoints and counters (hints persisted/applied/failed; anti-entropy diffs; RPC latencies)
- Add gossip hook to trigger immediate `ApplyHints` on member up events
- Admin endpoint to trigger manual hint application or anti-entropy for a key range
- TTL/GC policy tuning for hints; operational tooling for inspecting hint queues

---

## License

This codebase is intended for educational purposes. See repository for license information (if provided).
