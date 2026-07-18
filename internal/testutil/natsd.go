// Package testutil provides helpers shared by tests. It is only imported from
// _test files, so the embedded NATS server never reaches a production binary.
package testutil

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// NATS is an embedded JetStream-enabled server that can be stopped and
// restarted on the same port and store to exercise reconnection and recovery.
type NATS struct {
	t    testing.TB
	srv  *server.Server
	port int
	dir  string
}

// StartNATS launches an embedded server with JetStream and file storage in a
// temp directory. It is shut down automatically at test end.
func StartNATS(t testing.TB) *NATS {
	t.Helper()
	n := &NATS{t: t, dir: t.TempDir(), port: -1}
	n.start()
	t.Cleanup(n.Stop)
	return n
}

func (n *NATS) start() {
	n.t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: n.port, JetStream: true, StoreDir: n.dir,
		NoLog: true, NoSigs: true,
	})
	if err != nil {
		n.t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		n.t.Fatal("nats server not ready")
	}
	n.srv = srv
	n.port = srv.Addr().(*net.TCPAddr).Port
}

// URL is the client connection URL.
func (n *NATS) URL() string { return fmt.Sprintf("nats://127.0.0.1:%d", n.port) }

// Stop shuts the server down; persisted streams survive for Restart.
func (n *NATS) Stop() {
	if n.srv != nil {
		n.srv.Shutdown()
		n.srv.WaitForShutdown()
		n.srv = nil
	}
}

// Restart brings the server back on the same port with the same store.
func (n *NATS) Restart() {
	n.t.Helper()
	n.Stop()
	n.start()
}
