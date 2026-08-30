package rpc

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/homura/core-bank/raft"
)

// Transport security and client authentication (G4).
//
// Raft's safety argument assumes every message comes from a genuine cluster
// member: §2 puts Byzantine faults explicitly out of scope. An unauthenticated
// inter-node port turns that assumption into a wish. Figure 2's unconditional
// rule — "if RPC request or response contains term T > currentTerm, set
// currentTerm = T, convert to follower" — means an attacker who can reach the
// port sends one AppendEntries at a high term and every honest node obeys. The
// attacker becomes leader by the protocol's own rules and can rewrite the ledger.
//
// So this file is not a security nicety bolted onto a correct system. It is what
// makes the correctness proof apply at all. Theory logged in
// learn/READING_LIST.md §13 per RULES.md rule 1.
//
// Two distinct trust relationships, deliberately kept separate:
//
//   - node <-> node: MUTUAL TLS. Both sides present certificates. A peer's
//     identity is its certificate subject, not a self-asserted field in the
//     message, which is what closes forged AppendEntries structurally — the
//     forgery is rejected at the transport, before any Raft code runs.
//   - client -> node: a bearer token, checked before any command is proposed.
//     Deliberately NOT per-account authorization: that is application-level and
//     belongs with the bank domain. The goal here is only that unauthorized
//     callers cannot reach the cluster at all.

// TLSConfig describes the certificate material for one node.
//
// Empty means TLS is disabled, which is permitted only because the existing test
// suite and the local-development flow predate this. node/ warns loudly when it
// starts without TLS: a bank running plaintext should never be able to claim it
// did not know.
type TLSConfig struct {
	// CertFile and KeyFile are this node's own certificate and private key.
	CertFile string
	KeyFile  string

	// CAFile is the certificate authority that signs every node's certificate.
	// Peers are verified against it, and nothing else is trusted.
	CAFile string
}

// Enabled reports whether TLS material was configured.
func (c TLSConfig) Enabled() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.CAFile != ""
}

// Validate checks that a partially-specified TLS config is rejected at startup.
//
// Half-configured TLS is the dangerous case: a node given a certificate but no CA
// would happily run without verifying anyone. Following the existing -peers
// precedent, an unsafe configuration is a startup failure, not a warning.
func (c TLSConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}
	missing := ""
	switch {
	case c.CertFile == "":
		missing = "-tls-cert"
	case c.KeyFile == "":
		missing = "-tls-key"
	case c.CAFile == "":
		missing = "-tls-ca"
	}
	if missing != "" {
		return fmt.Errorf("rpc: TLS is partially configured: %s is missing. "+
			"Running with a certificate but no CA would accept any peer, so this is "+
			"refused rather than downgraded", missing)
	}
	for _, f := range []string{c.CertFile, c.KeyFile, c.CAFile} {
		if _, err := os.Stat(f); err != nil {
			return fmt.Errorf("rpc: TLS file %s: %w", f, err)
		}
	}
	return nil
}

// serverTLS builds the listener-side config: mutual TLS, verifying the client.
//
// ClientAuth is RequireAndVerifyClientCert, not VerifyClientCertIfGiven. The
// weaker setting authenticates only callers that volunteer a certificate, which
// is the same as no authentication at all for an attacker who simply omits one.
func (c TLSConfig) serverTLS() (*tls.Config, error) {
	cert, pool, err := c.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// clientTLS builds the dialer-side config.
func (c TLSConfig) clientTLS() (*tls.Config, error) {
	cert, pool, err := c.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func (c TLSConfig) load() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("rpc: load key pair: %w", err)
	}
	ca, err := os.ReadFile(c.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("rpc: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return tls.Certificate{}, nil, fmt.Errorf("rpc: CA file %s contains no usable certificate", c.CAFile)
	}
	return cert, pool, nil
}

// peerIdentity extracts the node id a TLS peer proved it owns.
//
// The identity is the certificate's Common Name. Returning it lets the caller
// enforce that the peer is the node it was expected to be — a valid certificate
// for the WRONG node id must still be rejected, or any cluster member could
// impersonate any other and a single compromised follower could forge the
// leader's messages.
func peerIdentity(conn net.Conn) (raft.NodeID, bool) {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return "", false
	}
	st := tc.ConnectionState()
	if len(st.PeerCertificates) == 0 {
		return "", false
	}
	return raft.NodeID(st.PeerCertificates[0].Subject.CommonName), true
}

// verifyPeerIdentity checks a dialled connection belongs to the expected node.
//
// TLS verification alone proves only that the peer holds a certificate signed by
// our CA — which every node in the cluster does. Binding the connection to the
// SPECIFIC id we meant to dial is what stops one member impersonating another.
func verifyPeerIdentity(conn net.Conn, want raft.NodeID) error {
	got, ok := peerIdentity(conn)
	if !ok {
		return fmt.Errorf("rpc: peer %s presented no certificate", want)
	}
	if got != want {
		return fmt.Errorf("rpc: peer identity mismatch: dialled %s but the certificate "+
			"identifies %s; a valid certificate for the wrong node is still an impostor", want, got)
	}
	return nil
}

// ErrUnauthenticated is returned to a client whose token is missing or wrong.
const ErrUnauthenticated = "rpc: unauthenticated: a valid client token is required"

// tokenAuth checks client bearer tokens.
//
// The zero value permits everything, which is what keeps existing tests and the
// local flow working. A configured token is required on every client call.
type tokenAuth struct {
	token string
}

// check compares a presented token in constant time.
//
// subtle.ConstantTimeCompare rather than ==: a byte-by-byte comparison leaks the
// length of the matching prefix through timing, which is enough to recover a
// token one character at a time.
func (a tokenAuth) check(presented string) bool {
	if a.token == "" {
		return true // auth not configured
	}
	return subtle.ConstantTimeCompare([]byte(a.token), []byte(presented)) == 1
}

// required reports whether client authentication is switched on.
func (a tokenAuth) required() bool { return a.token != "" }
