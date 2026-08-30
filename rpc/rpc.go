// Package rpc carries Raft RPCs and the client API over a real network.
//
// NOW.md specifies gRPC + protobuf. This implementation uses Go's net/rpc with
// gob over TCP instead, for one reason: protobuf requires the protoc toolchain as
// an external build dependency, and the wire format is not what Phase 1 is trying
// to teach. The Transport interface is unchanged, so swapping in gRPC later is a
// drop-in replacement that touches no consensus code — which is the property that
// mattered about the abstraction in the first place.
//
// [project decision] recorded in context/DESIGN.md.
package rpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/homura/core-bank/raft"
)

// RaftService exposes a raft.Server's RPC handlers over the network.
type RaftService struct {
	srv *raft.Server
}

// AppendEntries is the net/rpc entry point for raft.AppendEntries.
func (s *RaftService) AppendEntries(args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = s.srv.AppendEntries(args)
	return nil
}

// RequestVote is the net/rpc entry point for raft.RequestVote.
func (s *RaftService) RequestVote(args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = s.srv.RequestVote(args)
	return nil
}

// Server hosts a node's RPC endpoints on a TCP listener.
type Server struct {
	rpcSrv   *rpc.Server
	listener net.Listener
	wg       sync.WaitGroup

	mu     sync.Mutex
	closed bool

	// tlsOn records whether this listener requires mutual TLS, for Status and for
	// the startup banner. A node must be able to report whether it is protected.
	tlsOn bool

	// conns tracks accepted connections so Close can actually close them.
	//
	// Closing only the listener left established connections serving RPCs
	// indefinitely: a node "shut down" for a rolling restart kept answering
	// AppendEntries over the leader's cached connection, so the leader committed
	// against a quorum that no longer existed.
	conns map[net.Conn]struct{}
}

// Listen starts a plaintext RPC server for one Raft group on addr.
//
// Plaintext is retained for local development and for the existing test suite.
// Production configurations pass a TLSConfig via ListenSecure — see
// learn/READING_LIST.md §13 for why an unauthenticated consensus port is a
// correctness problem and not merely a security one.
func Listen(addr string, raftSrv *raft.Server, client *ClientService) (*Server, error) {
	return ListenSecure(addr, raftSrv, client, TLSConfig{})
}

// ListenSecure starts an RPC server, optionally with mutual TLS.
//
// A single Raft group registers under the service name "Raft", which is what
// every existing caller and test expects. Hosting several shards on one listener
// is ListenShards.
func ListenSecure(addr string, raftSrv *raft.Server, client *ClientService, tc TLSConfig) (*Server, error) {
	r := rpc.NewServer()
	if err := r.RegisterName("Raft", &RaftService{srv: raftSrv}); err != nil {
		return nil, fmt.Errorf("rpc: register raft: %w", err)
	}
	if client != nil {
		if err := r.RegisterName("Bank", client); err != nil {
			return nil, fmt.Errorf("rpc: register bank: %w", err)
		}
	}
	return listenOn(addr, r, tc)
}

// listenOn binds addr and starts serving an already-populated rpc.Server.
func listenOn(addr string, r *rpc.Server, tc TLSConfig) (*Server, error) {
	if err := tc.Validate(); err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rpc: listen %s: %w", addr, err)
	}

	if tc.Enabled() {
		cfg, err := tc.serverTLS()
		if err != nil {
			l.Close()
			return nil, err
		}
		// Wrapping the listener means the handshake — and therefore client
		// certificate verification — happens before ServeConn sees the connection.
		// An unauthenticated caller never reaches a Raft handler at all.
		l = tls.NewListener(l, cfg)
	}

	s := &Server{rpcSrv: r, listener: l, conns: make(map[net.Conn]struct{}), tlsOn: tc.Enabled()}
	s.wg.Add(1)
	go s.serve()
	return s, nil
}

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			continue
		}
		s.mu.Lock()
		if s.closed {
			// Raced with Close: refuse the connection rather than serving it.
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
				c.Close()
			}()
			s.rpcSrv.ServeConn(c)
		}(conn)
	}
}

// Addr returns the address actually bound, useful when addr specified port 0.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close stops the server: it stops accepting, closes every established
// connection, and waits for all handlers to finish.
//
// Closing only the listener is not enough. Established connections keep serving,
// and for a consensus RPC server that means a shut-down node keeps voting and
// acking replication — a phantom quorum.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	err := s.listener.Close()
	for _, c := range conns {
		c.Close() // unblocks the ServeConn goroutine parked on Read
	}
	s.wg.Wait()
	return err
}

// Transport dials peers over TCP and implements raft.Transport.
//
// Connections are cached per peer and re-dialed on failure. A failed dial is
// reported as raft.ErrUnreachable, which Raft treats as an ordinary condition —
// exactly as it treats a crashed node in the simulator.
//
// The cache lives in a *pool shared between every Transport addressing the same
// peers, so a process hosting several shards opens ONE connection per peer rather
// than one per shard (learn/READING_LIST.md §14).
type Transport struct {
	pool *pool

	// timeout bounds a single RPC. Without it a partitioned peer could block a
	// replication goroutine indefinitely.
	timeout time.Duration

	// service is the net/rpc service name this transport calls. A single-group
	// node uses "Raft"; a shard replica uses "Raft-<shard-id>", which is how many
	// independent Raft groups share one listener and one connection per peer.
	service string
}

// pool is the shared, mutex-guarded connection cache.
type pool struct {
	mu    sync.Mutex
	addrs map[raft.NodeID]string
	conns map[raft.NodeID]*rpc.Client

	// dials gates concurrent dials to the same peer: one dial per peer at a time,
	// with the shared mutex released while it runs.
	dials map[raft.NodeID]chan struct{}

	// tlsCfg, when enabled, makes every peer connection mutually authenticated.
	tlsCfg TLSConfig

	timeout time.Duration
}

// NewTransport creates a plaintext transport for a single Raft group.
func NewTransport(addrs map[raft.NodeID]string, timeout time.Duration) *Transport {
	return NewTransportSecure(addrs, timeout, TLSConfig{})
}

// NewTransportSecure creates a transport that dials peers over mutual TLS when
// tc is enabled.
func NewTransportSecure(addrs map[raft.NodeID]string, timeout time.Duration, tc TLSConfig) *Transport {
	return &Transport{
		pool: &pool{
			addrs:   addrs,
			conns:   make(map[raft.NodeID]*rpc.Client),
			dials:   make(map[raft.NodeID]chan struct{}),
			tlsCfg:  tc,
			timeout: timeout,
		},
		timeout: timeout,
		service: "Raft",
	}
}

// ForShard returns a transport addressing one shard's Raft group, sharing this
// transport's connection pool.
//
// Sharing the pool is the entire point of multiplexing: N shards hosted on a peer
// cost ONE connection, not N. CockroachDB and TiKV do the same for Ranges and
// Regions (learn/READING_LIST.md §14). Only the service name differs.
//
// The shared state lives in the *pool referenced by both transports rather than
// being copied. Copying a Transport that held its own mutex and maps would give
// each view a different lock guarding the same maps — a data race that stays
// silent until it corrupts one.
func (t *Transport) ForShard(id string) *Transport {
	return &Transport{pool: t.pool, timeout: t.timeout, service: "Raft-" + id}
}

func (p *pool) client(id raft.NodeID) (*rpc.Client, error) {
	// Fast path: an established connection.
	p.mu.Lock()
	if c, ok := p.conns[id]; ok && c != nil {
		p.mu.Unlock()
		return c, nil
	}
	addr, ok := p.addrs[id]
	if !ok {
		p.mu.Unlock()
		return nil, fmt.Errorf("rpc: unknown peer %s", id)
	}

	// Per-peer dial gate. Only one dial per peer runs at a time, and — critically
	// — the shared mutex is RELEASED while dialing.
	//
	// Dialing under the mutex meant one unreachable peer serialized every other
	// peer's RPCs behind its full dial timeout: a single dead node, the exact
	// failure Raft exists to survive, stalled the leader's entire replication
	// round. Measured at 2.95s to a HEALTHY peer while one unroutable peer was
	// dialing.
	gate, dialing := p.dials[id]
	if !dialing {
		gate = make(chan struct{})
		p.dials[id] = gate
	}
	p.mu.Unlock()

	if dialing {
		// Another goroutine is already dialing this peer; wait for it rather than
		// opening a second connection.
		<-gate
		p.mu.Lock()
		c, ok := p.conns[id]
		p.mu.Unlock()
		if ok && c != nil {
			return c, nil
		}
		return nil, raft.ErrUnreachable
	}

	conn, err := p.dial(id, addr)

	p.mu.Lock()
	delete(p.dials, id)
	if err != nil {
		p.mu.Unlock()
		close(gate)
		return nil, raft.ErrUnreachable
	}
	c := rpc.NewClient(conn)
	p.conns[id] = c
	p.mu.Unlock()
	close(gate)
	return c, nil
}

// dial opens one peer connection, over mutual TLS when configured.
//
// When TLS is on, the handshake is completed and the peer's identity is checked
// HERE, before the connection is cached. TLS verification alone proves only that
// the peer holds a certificate signed by our CA — which every node does. Binding
// the connection to the specific id we meant to dial is what stops one member
// impersonating another (learn/READING_LIST.md §13).
func (p *pool) dial(id raft.NodeID, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return nil, err
	}
	if !p.tlsCfg.Enabled() {
		return conn, nil
	}

	cfg, err := p.tlsCfg.clientTLS()
	if err != nil {
		conn.Close()
		return nil, err
	}
	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr == nil {
		cfg.ServerName = host
	}

	tc := tls.Client(conn, cfg)
	// Bound the handshake: without a deadline a peer that accepts the TCP
	// connection and then goes silent parks this goroutine indefinitely, which is
	// the same stall the dial timeout exists to prevent.
	if err := tc.SetDeadline(time.Now().Add(p.timeout)); err != nil {
		tc.Close()
		return nil, err
	}
	if err := tc.Handshake(); err != nil {
		tc.Close()
		return nil, err
	}
	if err := verifyPeerIdentity(tc, id); err != nil {
		tc.Close()
		return nil, err
	}
	// Clear the handshake deadline: it must not apply to ordinary RPC traffic.
	if err := tc.SetDeadline(time.Time{}); err != nil {
		tc.Close()
		return nil, err
	}
	return tc, nil
}

// drop discards a cached connection so the next call re-dials.
func (p *pool) drop(id raft.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[id]; ok && c != nil {
		c.Close()
	}
	delete(p.conns, id)
}

// call performs one RPC with a timeout.
//
// The reply the RPC decodes into is NOT the caller's. net/rpc writes the response
// body into the reply pointer whenever it arrives, which for an abandoned call is
// after this function has already returned — so sharing the caller's struct is a
// data race between the decoder and the caller reading its own variable.
//
// Found by -race under sustained sharded load (many groups, many in-flight
// calls), where timeouts are common enough to hit the window regularly. It was
// present before sharding; the extra concurrency only made it observable.
//
// So: decode into a scratch reply, and copy it to the caller ONLY on the success
// path, where nothing else can still be writing to it.
func (t *Transport) call(id raft.NodeID, method string, args, reply any, scratch any) error {
	return t.callWithTimeout(id, method, args, reply, scratch, t.timeout)
}

// callWithTimeout is call with an explicit budget, for RPCs that legitimately
// take longer than a heartbeat-sized one — a snapshot transfer, notably.
func (t *Transport) callWithTimeout(id raft.NodeID, method string, args, reply, scratch any,
	timeout time.Duration) error {
	c, err := t.pool.client(id)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- c.Call(method, args, scratch) }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil {
			// A genuine transport failure: the connection is unusable.
			t.pool.drop(id)
			return raft.ErrUnreachable
		}
		// The call completed, so the decoder is finished with scratch and this is
		// the only goroutine touching it. Safe to hand the value to the caller.
		copyReply(reply, scratch)
		return nil

	case <-timer.C:
		// Deliberately do NOT drop the connection here.
		//
		// Concurrent RPCs to one peer share a cached client. Dropping on timeout
		// closed the connection out from under every OTHER in-flight call, turning
		// them into spurious ErrUnreachable — measured at 91 of 200 healthy calls
		// failing because an unrelated call timed out. The leader then believed a
		// healthy follower was unreachable and backed off replication, exactly
		// under the load where that hurts most.
		//
		// net/rpc demultiplexes by sequence number, so the abandoned response is
		// discarded harmlessly when it arrives. The goroutine above exits then too,
		// because done is buffered. This is also what makes shard multiplexing safe
		// on a shared connection: a slow call for one shard does not stall another.
		return raft.ErrUnreachable
	}
}

// copyReply assigns *src to *dst for the two Raft reply types.
//
// A tiny type switch rather than reflection: there are exactly two reply types on
// this path, and the switch fails loudly for anything else instead of silently
// doing nothing.
func copyReply(dst, src any) {
	switch d := dst.(type) {
	case *raft.AppendEntriesReply:
		*d = *(src.(*raft.AppendEntriesReply))
	case *raft.RequestVoteReply:
		*d = *(src.(*raft.RequestVoteReply))
	case *raft.InstallSnapshotReply:
		*d = *(src.(*raft.InstallSnapshotReply))
	default:
		panic(fmt.Sprintf("rpc: copyReply does not handle %T", dst))
	}
}

// SendAppendEntries implements raft.Transport.
func (t *Transport) SendAppendEntries(to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply, scratch raft.AppendEntriesReply
	err := t.call(to, t.service+".AppendEntries", args, &reply, &scratch)
	return reply, err
}

// SendRequestVote implements raft.Transport.
func (t *Transport) SendRequestVote(to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply, scratch raft.RequestVoteReply
	err := t.call(to, t.service+".RequestVote", args, &reply, &scratch)
	return reply, err
}

// Close shuts all cached connections.
//
// Closing any Transport sharing a pool closes the pool: the connections are
// shared, so there is no meaningful per-shard close.
func (t *Transport) Close() {
	p := t.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, c := range p.conns {
		if c != nil {
			c.Close()
		}
		delete(p.conns, id)
	}
}

// ErrNoLeader is returned to a client when no leader is known.
var ErrNoLeader = errors.New("rpc: no known leader")
