#!/usr/bin/env bash
set -euo pipefail
DYN_SERVER=../bin/dynamo-server
DYN_CLIENT=../bin/dynamo-client
BASE_DIR=$(pwd)/antientropy_test
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR/data-nodeA" "$BASE_DIR/data-nodeB" "$BASE_DIR/data-nodeC" "$BASE_DIR/logs"

PEERS="nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003"

# start servers in background
$DYN_SERVER -id nodeA -port 7001 -peers "$PEERS" -data "$BASE_DIR/data-nodeA" > "$BASE_DIR/logs/nodeA.log" 2>&1 &
PID_A=$!
$DYN_SERVER -id nodeB -port 7002 -peers "$PEERS" -data "$BASE_DIR/data-nodeB" > "$BASE_DIR/logs/nodeB.log" 2>&1 &
PID_B=$!
$DYN_SERVER -id nodeC -port 7003 -peers "$PEERS" -data "$BASE_DIR/data-nodeC" > "$BASE_DIR/logs/nodeC.log" 2>&1 &
PID_C=$!

cleanup() {
  echo "shutting down servers..."
  kill -INT $PID_A $PID_B $PID_C || true
  wait $PID_A $PID_B $PID_C 2>/dev/null || true
}
trap cleanup EXIT

echo "waiting for servers to come up..."
sleep 2
# simple port-check loop
for p in 7001 7002 7003; do
  for i in {1..30}; do
    nc -z localhost $p && break
    sleep 0.2
  done
done

KEY="user:antientropy_bash_test"

echo "writing divergent values directly to replicas (nodeB and nodeC)..."
$DYN_CLIENT -addr localhost:7002 -svc replica -cmd put -key "$KEY" -val "VALUE-B" -timeout 10
$DYN_CLIENT -addr localhost:7003 -svc replica -cmd put -key "$KEY" -val "VALUE-C" -timeout 10

echo "initial reads (replicas):"
echo "nodeB:"
$DYN_CLIENT -addr localhost:7002 -svc replica -cmd get -key "$KEY" -timeout 10 || true
echo "nodeC:"
$DYN_CLIENT -addr localhost:7003 -svc replica -cmd get -key "$KEY" -timeout 10 || true

echo "sleeping to allow anti-entropy to run (adjust if your ticker is longer)..."
sleep 8

echo "post-antientropy reads (replicas):"
echo "nodeB:"
$DYN_CLIENT -addr localhost:7002 -svc replica -cmd get -key "$KEY" -timeout 10 || true
echo "nodeC:"
$DYN_CLIENT -addr localhost:7003 -svc replica -cmd get -key "$KEY" -timeout 10 || true

echo "tail logs for antientropy evidence (StreamMerkle / TransferItem / applied transferred):"
grep -E "StreamMerkle|TransferItem|applied transferred|merkle" -R "$BASE_DIR/logs" || true