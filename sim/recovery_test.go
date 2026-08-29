package sim

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/storage"
)

// State-machine recovery across restart.
//
// Figure 2 makes commitIndex and lastApplied volatile, but a state machine holding
// state a client has already been told about cannot start empty and wait. §7
// describes replay as the recovery mechanism ("as the log grows longer, it... takes
// more time to replay") — snapshotting exists because replay is what happens.
//
// For a bank this is not a performance detail. A 2PC participant that voted YES has
// made an unretractable promise; if it forgets that on restart, the funds it
// reserved become spendable again while the transaction is still live, and the
// coordinator can still commit. That is a double-spend.

// newPersistentServer builds a server backed by real files, so "restart" means
// building a new Server over the same storage.
func newPersistentServer(t *testing.T, dir string, id raft.NodeID, peers []raft.NodeID,
	sm raft.StateMachine) (*raft.Server, *storage.RaftState) {
	t.Helper()

	wal, err := storage.Open(filepath.Join(dir, string(id)+".wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	app, err := storage.OpenApplied(filepath.Join(dir, string(id)+".applied"))
	if err != nil {
		t.Fatalf("open applied: %v", err)
	}
	st := storage.NewRaftStateWithApplied(wal, app)

	srv := raft.NewServerWith(id, peers, sm, nil, raft.DefaultConfig(), 1)
	srv.SetStorage(st)
	return srv, st
}

// A single-node group commits immediately, which makes it the cleanest way to test
// apply-and-restart without election timing in the way.
func TestStateMachineRebuiltFromLogOnRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []raft.NodeID{"solo"}

	st1 := ledger.New()
	m1 := shard.NewMachine("shard-0", st1)
	srv1, store1 := newPersistentServer(t, dir, "solo", peers, m1)

	srv1.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv1.Role() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}
	if srv1.Role() != raft.Leader {
		t.Fatal("single-node server never became leader")
	}

	// Open an account and deposit, through the log.
	for _, c := range []ledger.Command{
		{Op: ledger.OpOpenAccount, IdempotencyKey: "o", To: "alice", Amount: 10000},
		{Op: ledger.OpDeposit, IdempotencyKey: "d1", To: "alice", Amount: 2500},
	} {
		idx, _, ok := srv1.Submit(shard.Command{Op: shard.OpSingle, Ledger: c}.Encode())
		if !ok {
			t.Fatal("leader rejected a submit")
		}
		<-srv1.WaitApplied(idx)
	}

	if bal, _ := st1.Balance("alice"); bal != 12500 {
		t.Fatalf("before restart alice = %s, want 125.00", bal)
	}
	srv1.Stop()
	store1.Close()

	// Restart: a brand-new Server and a brand-new, EMPTY ledger over the same files.
	st2 := ledger.New()
	m2 := shard.NewMachine("shard-0", st2)
	srv2, store2 := newPersistentServer(t, dir, "solo", peers, m2)
	defer store2.Close()

	if err := srv2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The ledger must be rebuilt from the log, before any election or replication.
	bal, found := st2.Balance("alice")
	if !found {
		t.Fatalf("alice does not exist after restart — the state machine was not rebuilt")
	}
	if bal != 12500 {
		t.Fatalf("after restart alice = %s, want 125.00 — replay is incomplete", bal)
	}
	if err := st2.VerifyDoubleEntry(); err != nil {
		t.Fatalf("rebuilt ledger fails its own audit: %v", err)
	}
}

// THE money bug this closes: a 2PC participant must not forget it voted YES.
func TestPreparedPromiseSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []raft.NodeID{"solo"}

	st1 := ledger.New()
	m1 := shard.NewMachine("shard-0", st1)
	srv1, store1 := newPersistentServer(t, dir, "solo", peers, m1)

	srv1.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv1.Role() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}

	submit := func(c shard.Command) {
		t.Helper()
		idx, _, ok := srv1.Submit(c.Encode())
		if !ok {
			t.Fatal("leader rejected a submit")
		}
		<-srv1.WaitApplied(idx)
	}

	submit(shard.Command{Op: shard.OpSingle, Ledger: ledger.Command{
		Op: ledger.OpOpenAccount, IdempotencyKey: "o", To: "alice", Amount: 10000}})

	// Vote YES on a cross-shard transfer: reserve 30.00 of alice's 100.00.
	transfer := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx1",
		From: "alice", To: "bob", Amount: 3000}
	submit(shard.Command{Op: shard.OpPrepare, TxID: "tx1", Ledger: transfer,
		Debit: true, Participants: []shard.ID{"shard-0", "shard-1"}})

	if r := st1.Reserved("alice"); r != 3000 {
		t.Fatalf("before restart reserved = %s, want 30.00", r)
	}
	if a, _ := st1.Available("alice"); a != 7000 {
		t.Fatalf("before restart available = %s, want 70.00", a)
	}
	if n := len(m1.InDoubt()); n != 1 {
		t.Fatalf("before restart in-doubt = %d, want 1", n)
	}
	srv1.Stop()
	store1.Close()

	// Restart.
	st2 := ledger.New()
	m2 := shard.NewMachine("shard-0", st2)
	srv2, store2 := newPersistentServer(t, dir, "solo", peers, m2)
	defer store2.Close()
	if err := srv2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The promise must have survived. Before this fix, all three of these were
	// zero: the participant forgot, and the reserved funds became spendable while
	// the coordinator could still commit the transfer.
	if r := st2.Reserved("alice"); r != 3000 {
		t.Fatalf("after restart reserved = %s, want 30.00 — the YES vote was forgotten, "+
			"so the promised funds are spendable again", r)
	}
	if a, _ := st2.Available("alice"); a != 7000 {
		t.Fatalf("after restart available = %s, want 70.00", a)
	}
	if n := len(m2.InDoubt()); n != 1 {
		t.Fatalf("after restart in-doubt = %d, want 1 — the participant no longer "+
			"knows it is mid-transaction", n)
	}

	// And the promised money must still be unspendable.
	res := st2.Apply(ledger.Command{Op: ledger.OpWithdraw,
		IdempotencyKey: "steal", From: "alice", Amount: 8000})
	if res.OK {
		t.Fatal("spent funds that were promised to an in-flight transfer, after a restart")
	}
}

// A committed decision must also survive, so recovery commits rather than aborts.
func TestDecisionRecordSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []raft.NodeID{"solo"}

	st1 := ledger.New()
	m1 := shard.NewMachine("shard-0", st1)
	srv1, store1 := newPersistentServer(t, dir, "solo", peers, m1)
	srv1.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && srv1.Role() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}

	submit := func(c shard.Command) {
		t.Helper()
		idx, _, ok := srv1.Submit(c.Encode())
		if !ok {
			t.Fatal("rejected")
		}
		<-srv1.WaitApplied(idx)
	}

	submit(shard.Command{Op: shard.OpSingle, Ledger: ledger.Command{
		Op: ledger.OpOpenAccount, IdempotencyKey: "o", To: "alice", Amount: 10000}})

	transfer := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx2",
		From: "alice", To: "bob", Amount: 2000}
	parts := []shard.ID{"shard-0", "shard-1"}
	submit(shard.Command{Op: shard.OpPrepare, TxID: "tx2", Ledger: transfer, Debit: true, Participants: parts})
	submit(shard.Command{Op: shard.OpDecision, TxID: "tx2", Ledger: transfer,
		Commit: true, Debit: true, Participants: parts})

	srv1.Stop()
	store1.Close()

	st2 := ledger.New()
	m2 := shard.NewMachine("shard-0", st2)
	srv2, store2 := newPersistentServer(t, dir, "solo", peers, m2)
	defer store2.Close()
	if err := srv2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	commit, known := m2.Decision("tx2")
	if !known {
		t.Fatal("the durable COMMIT decision was lost across restart — recovery would " +
			"wrongly abort a transaction that was already decided to commit")
	}
	if !commit {
		t.Fatal("decision came back as ABORT, want COMMIT")
	}
}
