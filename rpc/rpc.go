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

	// conns tracks accepted connections so Close can actually close them.
	//
	// Closing only the listener left established connections serving RPCs
	// indefinitely: a node "shut down" for a rolling restart kept answering
	// AppendEntries over the leader's cached connection, so the leader committed
	// against a quorum that no longer existed.
	conns map[net.Conn]struct{}
}

// Listen starts an RPC server for the given Raft server on addr.
func Listen(addr string, raftSrv *raft.Server, client *ClientService) (*Server, error) {
	r := rpc.NewServer()
	if err := r.RegisterName("Raft", &RaftService{srv: raftSrv}); err != nil {
		return nil, fmt.Errorf("rpc: register raft: %w", err)
	}
	if client != nil {
		if err := r.RegisterName("Bank", client); err != nil {
			return nil, fmt.Errorf("rpc: register bank: %w", err)
		}
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rpc: listen %s: %w", addr, err)
	}

	s := &Server{rpcSrv: r, listener: l, conns: make(map[net.Conn]struct{})}
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
type Transport struct {
	mu    sync.Mutex
	addrs map[raft.NodeID]string
	conns map[raft.NodeID]*rpc.Client

	// timeout bounds a single RPC. Without it a partitioned peer could block a
	// replication goroutine indefinitely.
	timeout time.Duration
}

// NewTransport creates a transport for the given peer addresses.
func NewTransport(addrs map[raft.NodeID]string, timeout time.Duration) *Transport {
	return &Transport{
		addrs:   addrs,
		conns:   make(map[raft.NodeID]*rpc.Client),
		timeout: timeout,
	}
}

func (t *Transport) client(id raft.NodeID) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.conns[id]; ok && c != nil {
		return c, nil
	}
	addr, ok := t.addrs[id]
	if !ok {
		return nil, fmt.Errorf("rpc: unknown peer %s", id)
	}

	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	c := rpc.NewClient(conn)
	t.conns[id] = c
	return c, nil
}

// drop discards a cached connection so the next call re-dials.
func (t *Transport) drop(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[id]; ok && c != nil {
		c.Close()
	}
	delete(t.conns, id)
}

// call performs one RPC with a timeout.
func (t *Transport) call(id raft.NodeID, method string, args, reply any) error {
	c, err := t.client(id)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- c.Call(method, args, reply) }()

	select {
	case err := <-done:
		if err != nil {
			t.drop(id)
			return raft.ErrUnreachable
		}
		return nil
	case <-time.After(t.timeout):
		// The call may still complete later; drop the connection so we do not
		// reuse one with a pending response.
		t.drop(id)
		return raft.ErrUnreachable
	}
}

// SendAppendEntries implements raft.Transport.
func (t *Transport) SendAppendEntries(to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := t.call(to, "Raft.AppendEntries", args, &reply)
	return reply, err
}

// SendRequestVote implements raft.Transport.
func (t *Transport) SendRequestVote(to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := t.call(to, "Raft.RequestVote", args, &reply)
	return reply, err
}

// Close shuts all cached connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.conns {
		if c != nil {
			c.Close()
		}
		delete(t.conns, id)
	}
}

// ErrNoLeader is returned to a client when no leader is known.
var ErrNoLeader = errors.New("rpc: no known leader")
