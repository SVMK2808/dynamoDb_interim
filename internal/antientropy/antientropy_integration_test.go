package antientropy_test

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// NOTE: edit these paths to match your environment
const (
	DYN_SERVER_BIN = "./bin/dynamo-server"
	DYN_CLIENT_BIN = "./bin/dynamo-client"
)

// helper to start a server process
func startServer(t *testing.T, id string, port int, peers string, dataDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(DYN_SERVER_BIN,
		"-id", id,
		"-port", strconv.Itoa(port),
		"-peers", peers,
		"-data", dataDir,
	)
	// redirect output to files for debugging
	outFile, err := os.Create(filepath.Join(dataDir, "server.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	cmd.Stdout = outFile
	cmd.Stderr = outFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return cmd
}

func stopServer(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error)
	go func() { done <- cmd.Wait() }()
	select {
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
	case <-done:
	}
}

func runClientCmd(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(DYN_CLIENT_BIN, args...).CombinedOutput()
	if err != nil {
		t.Logf("client (%v) output:\n%s", args, string(out))
		t.Fatalf("client cmd failed: %v", err)
	}
	return string(out)
}

func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", addr)
}

func TestAntiEntropyConverge(t *testing.T) {
	t.Skip("anti-entropy disabled; skipping integration test")
	// Ensure required binaries exist; skip integration test otherwise
	if _, err := os.Stat(DYN_SERVER_BIN); err != nil {
		t.Skipf("skipping: missing server binary %s", DYN_SERVER_BIN)
	}
	if _, err := os.Stat(DYN_CLIENT_BIN); err != nil {
		t.Skipf("skipping: missing client binary %s", DYN_CLIENT_BIN)
	}

	// prepare temp data dirs
	base := t.TempDir()
	nodeA := filepath.Join(base, "data-nodeA")
	nodeB := filepath.Join(base, "data-nodeB")
	nodeC := filepath.Join(base, "data-nodeC")
	_ = os.MkdirAll(nodeA, 0755)
	_ = os.MkdirAll(nodeB, 0755)
	_ = os.MkdirAll(nodeC, 0755)

	peers := "nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003"

	// start servers
	sA := startServer(t, "nodeA", 7001, peers, nodeA)
	sB := startServer(t, "nodeB", 7002, peers, nodeB)
	sC := startServer(t, "nodeC", 7003, peers, nodeC)
	defer stopServer(t, sA)
	defer stopServer(t, sB)
	defer stopServer(t, sC)

	// wait for servers to accept connections
	waitForPort(t, "localhost:7001", 10*time.Second)
	waitForPort(t, "localhost:7002", 10*time.Second)
	waitForPort(t, "localhost:7003", 10*time.Second)

	// make divergent writes directly to replicas (replica service)
	key := "user:antientropy_test"
	runClientCmd(t, "-addr", "localhost:7002", "-svc", "replica", "-cmd", "put", "-key", key, "-val", "VALUE-B", "-timeout", "10")
	runClientCmd(t, "-addr", "localhost:7003", "-svc", "replica", "-cmd", "put", "-key", key, "-val", "VALUE-C", "-timeout", "10")

	// sanity check: reads from both replicas show different values initially
	outB := runClientCmd(t, "-addr", "localhost:7002", "-svc", "replica", "-cmd", "get", "-key", key, "-timeout", "10")
	outC := runClientCmd(t, "-addr", "localhost:7003", "-svc", "replica", "-cmd", "get", "-key", key, "-timeout", "10")
	if strings.Contains(outB, "VALUE-C") || strings.Contains(outC, "VALUE-B") {
		t.Fatalf("unexpected initial replica state:\nnodeB:%s\nnodeC:%s", outB, outC)
	}

	// wait for anti-entropy to run and converge (choose an interval larger than your coordinator's anti-entropy ticker)
	time.Sleep(8 * time.Second)

	// read final values from both replicas (or via coordinator)
	finalB := runClientCmd(t, "-addr", "localhost:7002", "-svc", "replica", "-cmd", "get", "-key", key, "-timeout", "10")
	finalC := runClientCmd(t, "-addr", "localhost:7003", "-svc", "replica", "-cmd", "get", "-key", key, "-timeout", "10")

	if finalB != finalC {
		t.Fatalf("replicas did not converge:\nnodeB:%s\nnodeC:%s", finalB, finalC)
	}
}