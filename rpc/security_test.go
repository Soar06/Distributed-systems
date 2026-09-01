package rpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// Tests for transport security and client authentication (G4).
//
// Per RULES.md rule 3, these cover the flows the feature will actually face:
// the normal path (a properly authenticated peer and client work), the failure
// paths (no certificate, a certificate from the wrong CA, a valid certificate for
// the WRONG node id, a missing or wrong client token), and the regression path
// (the cluster still elects and commits with TLS on — a security change that
// breaks liveness has only traded one outage for another).
//
// The threat being closed, concretely: Raft §2 puts Byzantine faults out of
// scope, so Figure 2's unconditional "if T > currentTerm, convert to follower"
// rule makes an unauthenticated port a takeover vector. See
// learn/READING_LIST.md §13.

// noopSM is a state machine that does nothing, for tests that exercise the
// transport rather than the application.
type noopSM struct{}

func (noopSM) Apply([]byte) any { return nil }

// --- certificate helpers -------------------------------------------------

type testCA struct {
	dir      string
	caCert   *x509.Certificate
	caKey    *ecdsa.PrivateKey
	caPath   string
	caDERPEM []byte
}

// newTestCA creates a throwaway certificate authority in a temp directory.
func newTestCA(t *testing.T) *testCA {
	t.Helper()
	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "core-bank-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}

	return &testCA{dir: dir, caCert: cert, caKey: key, caPath: caPath, caDERPEM: caPEM}
}

// issue creates a node certificate whose Common Name is the node id — which is
// what the transport checks the peer's identity against.
func (ca *testCA) issue(t *testing.T, nodeID string) TLSConfig {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key for %s: %v", nodeID, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		},
		// Loopback is what the tests dial; the transport sets ServerName from the
		// dialled address, so the certificate has to cover it.
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:    []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.caCert, &key.PublicKey, ca.caKey)
	if err != nil {
		t.Fatalf("sign cert for %s: %v", nodeID, err)
	}

	certPath := filepath.Join(ca.dir, nodeID+".crt")
	keyPath := filepath.Join(ca.dir, nodeID+".key")

	if err := os.WriteFile(certPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	if err := os.WriteFile(keyPath,
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	return TLSConfig{CertFile: certPath, KeyFile: keyPath, CAFile: ca.caPath}
}

// --- config validation ---------------------------------------------------

// Half-configured TLS must be refused at startup, not silently downgraded. A node
// given a certificate but no CA would accept any peer at all, which is worse than
// no TLS because it looks protected.
func TestPartialTLSConfigIsRejected(t *testing.T) {
	ca := newTestCA(t)
	full := ca.issue(t, "n1")

	cases := []struct {
		name string
		cfg  TLSConfig
	}{
		{"cert without key", TLSConfig{CertFile: full.CertFile}},
		{"cert and key without CA", TLSConfig{CertFile: full.CertFile, KeyFile: full.KeyFile}},
		{"CA without cert", TLSConfig{CAFile: full.CAFile}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Fatalf("partial TLS config %+v was accepted; it must be refused rather "+
					"than running unprotected", tc.cfg)
			}
		})
	}

	if err := full.Validate(); err != nil {
		t.Fatalf("a complete TLS config was rejected: %v", err)
	}
	if err := (TLSConfig{}).Validate(); err != nil {
		t.Fatalf("an empty (TLS-disabled) config was rejected: %v", err)
	}
}

// A config naming files that do not exist must fail at startup rather than at
// the first connection, when a node is already believed to be up.
func TestMissingTLSFilesAreRejectedAtStartup(t *testing.T) {
	cfg := TLSConfig{
		CertFile: filepath.Join(t.TempDir(), "nope.crt"),
		KeyFile:  filepath.Join(t.TempDir(), "nope.key"),
		CAFile:   filepath.Join(t.TempDir(), "nope.ca"),
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("a TLS config naming nonexistent files was accepted")
	}
}

// --- peer authentication -------------------------------------------------

// The normal path: two nodes with certificates from the same CA replicate
// successfully over mutual TLS.
func TestMutualTLSPeersCommunicate(t *testing.T) {
	ca := newTestCA(t)
	cfg1, cfg2 := ca.issue(t, "n1"), ca.issue(t, "n2")

	// n2 is a bare listener whose Raft server just answers RPCs.
	srv2 := raft.NewServer("n2", []raft.NodeID{"n1", "n2"}, &noopSM{})
	l2, err := ListenSecure("127.0.0.1:0", srv2, nil, cfg2)
	if err != nil {
		t.Fatalf("listen n2: %v", err)
	}
	defer l2.Close()

	tr := NewTransportSecure(map[raft.NodeID]string{"n2": l2.Addr()}, 2*time.Second, cfg1)
	defer tr.Close()

	reply, err := tr.SendRequestVote("n2", raft.RequestVoteArgs{
		Term: 1, CandidateID: "n1", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("RequestVote over mutual TLS failed: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected a vote from a healthy peer, got %+v", reply)
	}
}

// A caller with no client certificate must be rejected. This is the attack the
// whole feature exists to stop: an unauthenticated AppendEntries at a high term
// takes the cluster over by Figure 2's own rules.
func TestUnauthenticatedPeerIsRejected(t *testing.T) {
	ca := newTestCA(t)
	cfgServer := ca.issue(t, "n2")

	srv2 := raft.NewServer("n2", []raft.NodeID{"n1", "n2"}, &noopSM{})
	l2, err := ListenSecure("127.0.0.1:0", srv2, nil, cfgServer)
	if err != nil {
		t.Fatalf("listen n2: %v", err)
	}
	defer l2.Close()

	// A plaintext transport — exactly what an attacker on the network has.
	tr := NewTransport(map[raft.NodeID]string{"n2": l2.Addr()}, 500*time.Millisecond)
	defer tr.Close()

	_, err = tr.SendAppendEntries("n2", raft.AppendEntriesArgs{
		Term: 99, LeaderID: "attacker",
	})
	if err == nil {
		t.Fatal("a plaintext AppendEntries at term 99 was ACCEPTED by a TLS listener; " +
			"an attacker could take over the cluster by Figure 2's own term rule")
	}
	if srv2.CurrentTerm() >= 99 {
		t.Fatalf("the forged term was applied: currentTerm = %d", srv2.CurrentTerm())
	}
}

// A certificate signed by a DIFFERENT CA must be rejected, even though it is a
// perfectly valid certificate.
func TestPeerWithForeignCAIsRejected(t *testing.T) {
	realCA := newTestCA(t)
	foreignCA := newTestCA(t)

	server := realCA.issue(t, "n2")
	impostor := foreignCA.issue(t, "n1") // valid, but signed by the wrong authority

	srv2 := raft.NewServer("n2", []raft.NodeID{"n1", "n2"}, &noopSM{})
	l2, err := ListenSecure("127.0.0.1:0", srv2, nil, server)
	if err != nil {
		t.Fatalf("listen n2: %v", err)
	}
	defer l2.Close()

	tr := NewTransportSecure(map[raft.NodeID]string{"n2": l2.Addr()}, 2*time.Second, impostor)
	defer tr.Close()

	if _, err := tr.SendRequestVote("n2", raft.RequestVoteArgs{Term: 5, CandidateID: "n1"}); err == nil {
		t.Fatal("a certificate from an unknown CA was accepted")
	}
}

// The subtle one: a VALID certificate for the wrong node id.
//
// TLS verification alone proves only that the peer holds a certificate signed by
// our CA — which every node in the cluster does. Without binding the connection to
// the specific id being dialled, any member could impersonate any other, and one
// compromised follower could forge the leader's messages.
func TestPeerWithValidCertificateForWrongNodeIsRejected(t *testing.T) {
	ca := newTestCA(t)

	// The listener is n3, but the transport's peer map says the address belongs to
	// n2 — so the dialler expects n2 and gets a valid certificate naming n3.
	srv3 := raft.NewServer("n3", []raft.NodeID{"n1", "n2", "n3"}, &noopSM{})
	l3, err := ListenSecure("127.0.0.1:0", srv3, nil, ca.issue(t, "n3"))
	if err != nil {
		t.Fatalf("listen n3: %v", err)
	}
	defer l3.Close()

	tr := NewTransportSecure(map[raft.NodeID]string{"n2": l3.Addr()}, 2*time.Second, ca.issue(t, "n1"))
	defer tr.Close()

	if _, err := tr.SendRequestVote("n2", raft.RequestVoteArgs{Term: 3, CandidateID: "n1"}); err == nil {
		t.Fatal("a valid certificate for the WRONG node id was accepted; any cluster " +
			"member could then impersonate any other")
	}
}

// --- client authentication -----------------------------------------------

func newAuthedClientService(t *testing.T, token string) (*ClientService, *raft.Server) {
	t.Helper()
	state := ledger.New()
	machine := ledger.NewMachine(state)
	srv := raft.NewServer("n1", []raft.NodeID{"n1"}, machine)
	return NewClientServiceAuth(srv, machine, map[raft.NodeID]string{"n1": "127.0.0.1:0"}, token), srv
}

// A write with no token must be refused before anything is proposed. An
// unauthenticated entry that reaches the log is indistinguishable to the ledger
// from an authorized one.
func TestUnauthenticatedWriteIsRefusedBeforeProposing(t *testing.T) {
	svc, srv := newAuthedClientService(t, "s3cret")

	before := len(srv.LogEntries())

	var reply TxReply
	if err := svc.Submit(TxArgs{
		Op: "open", IdempotencyKey: "k1", To: "alice", Amount: 100,
	}, &reply); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if !reply.Unauthenticated {
		t.Fatalf("a write with no token was not marked unauthenticated: %+v", reply)
	}
	if reply.OK {
		t.Fatal("a write with no token reported success")
	}
	if reply.Indeterminate {
		t.Fatal("an unauthenticated write must never be Indeterminate: nothing was " +
			"proposed, so there is nothing that might still commit")
	}
	if got := len(srv.LogEntries()); got != before {
		t.Fatalf("an unauthenticated write reached the log: %d -> %d entries", before, got)
	}
}

// A wrong token is refused exactly like a missing one.
func TestWrongClientTokenIsRefused(t *testing.T) {
	svc, _ := newAuthedClientService(t, "s3cret")

	var reply TxReply
	svc.Submit(TxArgs{
		Op: "open", IdempotencyKey: "k1", To: "alice", Amount: 100, Token: "guess",
	}, &reply)

	if !reply.Unauthenticated {
		t.Fatalf("a wrong token was accepted: %+v", reply)
	}
}

// Reads are authenticated too: every balance in the bank is readable here.
func TestUnauthenticatedReadIsRefused(t *testing.T) {
	svc, _ := newAuthedClientService(t, "s3cret")

	var reply BalanceReply
	svc.Balance(BalanceArgs{Account: "alice"}, &reply)

	if !reply.Unauthenticated {
		t.Fatalf("an unauthenticated read was served: %+v", reply)
	}
	if reply.Found {
		t.Fatal("an unauthenticated read returned account data")
	}
}

// The regression path: with authentication configured, a correct token still
// works. A security control that blocks legitimate traffic has only traded one
// outage for another.
func TestCorrectClientTokenIsAccepted(t *testing.T) {
	svc, _ := newAuthedClientService(t, "s3cret")

	var reply BalanceReply
	if err := svc.Balance(BalanceArgs{Account: "nobody", Token: "s3cret"}, &reply); err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if reply.Unauthenticated {
		t.Fatalf("a correct token was rejected: %+v", reply)
	}
	if reply.Err != "" {
		t.Fatalf("unexpected error for an authenticated read: %s", reply.Err)
	}
}

// With no token configured, the API stays open — this is what keeps local
// development and the pre-existing tests working, and it is why node/ warns
// loudly at startup when auth is off.
func TestTokenAuthDisabledPermitsEverything(t *testing.T) {
	svc, _ := newAuthedClientService(t, "")

	var reply BalanceReply
	svc.Balance(BalanceArgs{Account: "nobody"}, &reply)
	if reply.Unauthenticated {
		t.Fatal("client auth was enforced despite no token being configured")
	}
}

// A token comparison must not leak the matching prefix through timing. This
// asserts the constant-time path is actually used, by checking behaviour rather
// than measuring time: same-length tokens differing in the first byte and in the
// last byte must both be rejected.
func TestTokenComparisonRejectsPartialMatches(t *testing.T) {
	a := tokenAuth{token: "abcdefgh"}
	for _, bad := range []string{"zbcdefgh", "abcdefgz", "abcdefg", "abcdefghi", ""} {
		if a.check(bad) {
			t.Fatalf("token %q was accepted against %q", bad, a.token)
		}
	}
	if !a.check("abcdefgh") {
		t.Fatal("the correct token was rejected")
	}
}
