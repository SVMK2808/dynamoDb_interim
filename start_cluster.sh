#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./start_cluster.sh N R W
# Example:
#   ./start_cluster.sh 3 2 2

# --- config: change these if your binary differs ---
NODE_BINARY="./node"            # path to your node binary
BASE_PORT=7001                  # first port
LOG_DIR="./logs"                # logs dir
START_TIMEOUT=20                # seconds to wait per node
# If your node accepts CLI flags for R/W, include them in NODE_ARGS_TEMPLATE.
# Example template contains placeholders: {R} {W} {NODE_ID} {PORT} {PEERS}
NODE_ARGS_TEMPLATE="--node-id {NODE_ID} --listen localhost:{PORT} --consistency-r {R} --consistency-w {W} --peers {PEERS}"

# ----------------------------------------------------------------
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 N R W"
  echo "Example: $0 3 2 2"
  exit 1
fi

N="$1"
R="$2"
W="$3"

if ! [[ "$N" =~ ^[0-9]+$ && "$R" =~ ^[0-9]+$ && "$W" =~ ^[0-9]+$ ]]; then
  echo "N, R and W must be integers"
  exit 1
fi

mkdir -p "$LOG_DIR"

# build list of nodes (id:addr)
declare -a NODES
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i - 1))
  id="node${i}"
  addr="localhost:${port}"
  NODES+=("${id}:${addr}")
done

# construct peers string (comma separated addresses)
PEERS_CSV=$(IFS=,; echo "${NODES[*]}")

# check binary
if [ ! -x "$NODE_BINARY" ]; then
  echo "WARNING: node binary '$NODE_BINARY' not found or not executable."
  echo "If you want to run your real node binary, build it and place it at $NODE_BINARY"
  echo "Proceeding to start dummy HTTP servers on ports to simulate nodes (health checks only)."
  USE_DUMMY=1
else
  USE_DUMMY=0
fi

PIDS_FILE=".cluster_pids"
: > "$PIDS_FILE"

# start nodes
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i - 1))
  node_id="node${i}"
  logf="${LOG_DIR}/node-${i}.log"

  if [ "$USE_DUMMY" -eq 1 ]; then
    # start a lightweight python HTTP server to occupy the port (simulate node up)
    # also create a simple /health endpoint via a small Python one-liner
    nohup python3 -u -c "
import http.server, socketserver, sys
class H(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200); self.end_headers(); self.wfile.write(b'OK')
        else:
            self.send_response(404); self.end_headers()
PORT = $port
with socketserver.TCPServer(('localhost', PORT), H) as httpd:
    httpd.serve_forever()
" > "$logf" 2>&1 &

    pid=$!
    echo "$pid" >> "$PIDS_FILE"
    printf "Started dummy node %s on %s (pid=%d)  log=%s\n" "$node_id" "localhost:${port}" "$pid" "$logf"
  else
    # prepare command args by replacing placeholders in NODE_ARGS_TEMPLATE
    # allowed placeholders: {R} {W} {NODE_ID} {PORT} {PEERS}
    args="${NODE_ARGS_TEMPLATE//\{R\}/$R}"
    args="${args//\{W\}/$W}"
    args="${args//\{NODE_ID\}/$node_id}"
    args="${args//\{PORT\}/$port}"
    args="${args//\{PEERS\}/$PEERS_CSV}"

    # start node
    nohup $NODE_BINARY $args > "$logf" 2>&1 &
    pid=$!
    echo "$pid" >> "$PIDS_FILE"
    printf "Started node %s on %s (pid=%d)  log=%s\n" "$node_id" "localhost:${port}" "$pid" "$logf"
  fi
done

# wait for nodes to be up (tcp check)
echo
echo "Waiting up to ${START_TIMEOUT}s for each node to become reachable..."
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i - 1))
  node_id="node${i}"
  t=0
  success=0
  while [ $t -lt $START_TIMEOUT ]; do
    if command -v nc >/dev/null 2>&1; then
      nc -z localhost $port >/dev/null 2>&1 && success=1 && break
    else
      # fallback to curl health path (works if your node exposes /health) or try bash /dev/tcp
      if curl -s --max-time 1 "http://localhost:${port}/health" >/dev/null 2>&1; then success=1; break; fi
      if (echo > /dev/tcp/localhost/$port) >/dev/null 2>&1; then success=1; break; fi
    fi
    sleep 1
    t=$((t+1))
  done

  if [ "$success" -eq 1 ]; then
    printf "Node %s is reachable at localhost:%d\n" "$node_id" "$port"
  else
    printf "Warning: node %s NOT reachable at localhost:%d after %ds (check %s)\n" "$node_id" "$port" "$START_TIMEOUT" "${LOG_DIR}/node-${i}.log"
  fi
done

echo
echo "Cluster started:"
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i - 1))
  echo "  node$((i)): localhost:${port}"
done

echo
echo "Quorum parameters: N=${N}, R=${R}, W=${W}"
echo
echo "To stop the cluster:"
echo "  kill \$(cat $PIDS_FILE) || true; rm -f $PIDS_FILE"
echo
echo "Logs are in ${LOG_DIR}/node-*.log"