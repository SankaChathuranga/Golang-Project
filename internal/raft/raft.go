package raft

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/transport"
)

// ------------------------------------------------------------
// Raft node state & constructor
// ------------------------------------------------------------

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string { return [...]string{"Follower", "Candidate", "Leader"}[s] }

type Node struct {
	mu sync.RWMutex

	// identity & topology
	id    string
	peers map[string]string // peerID -> addr

	// persistent state and caching variables
	currentTerm int
	votedFor    string
	db          *bolt.DB
	log         StableLog
	store       StableStore

	// volatile state
	commitIndex int
	lastApplied int

	// leader-only state
	nextIndex  map[string]int
	matchIndex map[string]int

	// runtime plumbing
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	stopCh         chan struct{}
	logger         *log.Logger

	// transport
	trans *transport.HTTPTransport
}

func NewNode(id string, peers map[string]string, applyCh chan ApplyMsg, db *bolt.DB) (*Node, error) {
	store, err := NewBoltStore(db)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:         id,
		peers:      peers,
		db:         db,
		log:        NewBoltLog(db),
		store:      store,
		state:      Follower,
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
		logger:     log.New(log.Writer(), "[RAFT] ", log.LstdFlags),
	}

	term, err := n.store.Term()
	if err != nil {
		return nil, err
	}
	n.currentTerm = term

	votedFor, err := n.store.VotedFor()
	if err != nil {
		return nil, err
	}
	n.votedFor = votedFor

	lastApplied, err := n.store.LastApplied()
	if err != nil {
		return nil, err
	}
	n.lastApplied = lastApplied

	// Initialize commitIndex to lastApplied to prevent replay of old entries
	n.commitIndex = n.lastApplied

	n.resetElectionTimer()
	n.trans = transport.New(n.handleInbound)
	return n, nil
}

// ------------------------------------------------------------
// Public API
// ------------------------------------------------------------

func (n *Node) Serve(addr string) error {
	svr := &http.Server{Addr: addr, Handler: n.trans}
	go n.ticker()
	return svr.ListenAndServe()
}

func (n *Node) Start() {
	go n.ticker()
	go func() {
		addr := n.peers[n.id]
		if addr == "" {
			n.logger.Printf("node %s has no address configured", n.id)
			return
		}
		mux := http.NewServeMux()
		mux.Handle("/", n.trans)
		svr := &http.Server{Addr: addr, Handler: mux}
		if err := svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.logger.Printf("HTTP server error: %v", err)
		}
	}()
}

func (n *Node) Stop() { close(n.stopCh) }

// Propose replicates a command **only the leader**.
func (n *Node) Propose(cmd any) (idx int, ok bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return -1, false
	}
	idx = n.log.Append(LogEntry{Term: n.currentTerm, Command: cmd})

	// ---------- single-node fast commit ----------------
	if len(n.peers) == 0 {
		n.commitIndex = idx
		n.applyCommitted() // apply locally
		n.maybePrune()

		n.mu.Unlock()

		return idx, true
	}
	// ---------------------------------------------------

	n.mu.Unlock()

	go n.broadcastAppendEntries()
	return idx, true
}

// ------------------------------------------------------------
// Ticker goroutine – drives elections & heart-beats
// ------------------------------------------------------------

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()

			if !isLeader { // only followers / candidates start elections
				// run election logic in its *own* goroutine so the ticker
				// continues to service heart-beats and term updates.
				go n.startElection()
			}
		case <-n.heartbeatTimerC():
			n.mu.RLock()
			leader := n.state == Leader
			n.mu.RUnlock()
			if leader {
				n.broadcastAppendEntries()
			}
			n.heartbeatTimer.Reset(config.HeartbeatInterval)
		}
	}
}

func (n *Node) startHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
	} else {
		n.heartbeatTimer.Reset(config.HeartbeatInterval)
	}
}

func (n *Node) heartbeatTimerC() <-chan time.Time {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
		if !n.heartbeatTimer.Stop() {
			<-n.heartbeatTimer.C
		}
	}
	return n.heartbeatTimer.C
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(randomElectionTimeout())
	} else {
		n.electionTimer.Reset(randomElectionTimeout())
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// ------------------------------------------------------------
// Elections
// ------------------------------------------------------------

func (n *Node) startElection() {
	// step up to candidate & bump term
	n.mu.Lock()
	n.currentTerm++
	if err := n.store.SetTerm(n.currentTerm); err != nil {
		n.logger.Printf("failed to set term: %v", err)
	}
	n.state = Candidate
	n.votedFor = n.id
	if err := n.store.SetVotedFor(n.id); err != nil {
		n.logger.Printf("failed to set vote: %v", err)
	}
	term := n.currentTerm
	lastIdx, lastTerm := n.log.LastIndexTerm()
	n.resetElectionTimer()

	// Special case: single node cluster
	if len(n.peers) == 0 {
		n.becomeLeader()
		n.mu.Unlock()
		return
	}

	// copy peers map so we can iterate after releasing the lock
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.Unlock()

	var votes int32 = 1 // self-vote
	var wg sync.WaitGroup
	var highestTerm int32 = int32(term)
	var voteCh = make(chan bool, len(peerAddrs))

	for pid, paddr := range peerAddrs {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			args := RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
			var reply RequestVoteReply
			if err := n.trans.Call(addr, transport.RPCRequestVote, &args, &reply); err != nil {
				n.logger.Printf("failed to request vote from %s: %v", id, err)
				return
			}
			if reply.Term > term {
				atomic.StoreInt32(&highestTerm, int32(reply.Term))
				n.mu.Lock()
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				return
			}
			if reply.VoteGranted && reply.Term == term {
				voteCh <- true
			}
		}(pid, paddr)
	}

	// Wait for votes with timeout
	go func() {
		wg.Wait()
		close(voteCh)
	}()

	// Count votes
	for vote := range voteCh {
		if vote {
			if atomic.AddInt32(&votes, 1) > int32((len(n.peers)+1)/2) {
				n.mu.Lock()
				if n.state == Candidate && n.currentTerm == term && atomic.LoadInt32(&highestTerm) == int32(term) {
					n.becomeLeader()
				}
				n.mu.Unlock()
				return
			}
		}
	}
}

// ------------------------------------------------------------
// Leader transition helpers
// ------------------------------------------------------------

func (n *Node) becomeFollower(term int) {
	n.state = Follower

	n.currentTerm = term
	if err := n.store.SetTerm(term); err != nil {
		n.logger.Printf("failed to set term: %v", err)
	}

	n.votedFor = ""
	if err := n.store.SetVotedFor(""); err != nil {
		n.logger.Printf("failed to clear vote: %v", err)
	}

	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	// init nextIndex/matchIndex
	lastIdx := n.log.LastIndex() + 1
	for id := range n.peers {
		if id == n.id {
			continue
		}
		n.nextIndex[id] = lastIdx
		n.matchIndex[id] = 0
	}
	// send initial heart-beat outside the lock
	go n.broadcastAppendEntries()
	n.startHeartbeatTimer()
}

func (n *Node) broadcastAppendEntries() {
	// capture a *snapshot* of leader's state under read-lock
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	term := n.currentTerm
	commitIdx := n.commitIndex
	nextIdxSnap := make(map[string]int, len(n.nextIndex))
	for k, v := range n.nextIndex {
		nextIdxSnap[k] = v
	}
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.RUnlock()

	for pid, addr := range peerAddrs {
		ni := nextIdxSnap[pid]
		go func(id, paddr string, next int) {
			prevIdx := next - 1
			prevTerm := 0
			if prevIdx > 0 {
				if e, ok := n.log.At(prevIdx); ok {
					prevTerm = e.Term
				}
			}
			// gather entries [next .. last]
			entries := make([]LogEntry, 0)
			for i := next; i <= n.log.LastIndex(); i++ {
				if e, ok := n.log.At(i); ok {
					entries = append(entries, e)
				}
			}
			args := AppendEntriesArgs{Term: term, LeaderID: n.id, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: commitIdx}
			var reply AppendEntriesReply
			if err := n.trans.Call(paddr, transport.RPCAppendEntries, &args, &reply); err != nil {
				return // network error – ignore, follower will timeout
			}
			n.handleAppendEntriesReply(id, &reply)
		}(pid, addr, ni)
	}
}

func (n *Node) handleAppendEntriesReply(peerID string, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if !reply.Success {
		// Optimistic backoff: if we're far behind, jump back more aggressively
		if n.nextIndex[peerID] > 1 {
			// If we're very far behind, jump back more aggressively
			if n.nextIndex[peerID]-n.matchIndex[peerID] > 100 {
				n.nextIndex[peerID] = n.matchIndex[peerID] + 1
			} else {
				n.nextIndex[peerID]--
			}
		}
		return
	}

	// --- success path --------------------------------------------------
	n.nextIndex[peerID] = n.log.LastIndex() + 1
	n.matchIndex[peerID] = n.log.LastIndex()

	// Try to advance commitIndex
	advanced := false
	for i := n.commitIndex + 1; i <= n.log.LastIndex(); i++ {
		// Only commit entries from current term
		if e, ok := n.log.At(i); !ok || e.Term != n.currentTerm {
			continue
		}
		replicated := 1 // self
		for id := range n.peers {
			if id != n.id && n.matchIndex[id] >= i {
				replicated++
			}
		}
		if replicated > (len(n.peers)+1)/2 {
			n.commitIndex = i
			advanced = true
		}
	}

	// tell followers the new commitIndex
	if advanced {
		go n.broadcastAppendEntries()
		n.maybePrune()
	}

	// apply to local state machine
	n.applyCommitted()
}

func (n *Node) onRequestVote(args *RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	li, lt := n.log.LastIndexTerm()
	upToDate := args.LastLogTerm > lt || (args.LastLogTerm == lt && args.LastLogIndex >= li)

	grant := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		grant = true

		n.votedFor = args.CandidateID
		if err := n.store.SetVotedFor(args.CandidateID); err != nil {
			n.logger.Printf("failed to set vote: %v", err)
		}

		n.resetElectionTimer()
	}
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: grant}
}

func (n *Node) onAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	n.resetElectionTimer()

	// log consistency check
	if args.PrevLogIndex > n.log.LastIndex() {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.PrevLogIndex > 0 {
		if e, ok := n.log.At(args.PrevLogIndex); !ok || e.Term != args.PrevLogTerm {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	// append new entries (truncate conflicts first)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if e, ok := n.log.At(idx); !ok || e.Term != entry.Term {
			// truncate suffix
			err := n.log.TruncateSuffix(idx)
			if err != nil {
				panic(err)
			}
			// Append all remaining entries
			for j := i; j < len(args.Entries); j++ {
				n.log.Append(args.Entries[j])
			}
			break
		}
	}

	// Update commit index and apply entries
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min_(args.LeaderCommit, n.log.LastIndex())
		// Apply all newly committed entries immediately
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			if e, ok := n.log.At(n.lastApplied); ok {
				n.applyCh <- ApplyMsg{CommandValid: true, Command: e.Command,
					CommandIndex: n.lastApplied}
				if err := n.store.SetLastApplied(n.lastApplied); err != nil {
					n.logger.Printf("failed to set last applied: %v", err)
				}
			}
		}
	}

	n.maybePrune()

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) Trans() http.Handler { return n.trans }

func min_(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if e, ok := n.log.At(n.lastApplied); ok {
			n.applyCh <- ApplyMsg{CommandValid: true, Command: e.Command,
				CommandIndex: n.lastApplied}
			if err := n.store.SetLastApplied(n.lastApplied); err != nil {
				n.logger.Printf("failed to set last applied: %v", err)
			}
		}
	}
}

func (n *Node) maybePrune() {
	if n.commitIndex-n.log.FirstIndex() > config.PruneEvery {
		cutoff := n.commitIndex - config.RetainTail
		if cutoff > n.log.FirstIndex() {
			n.log.TruncateBefore(cutoff)
		}
	}
}

// ------------------------------------------------------------
// Inbound RPC handlers (HTTP callbacks)
// ------------------------------------------------------------

func (n *Node) handleInbound(method transport.RPC, body io.Reader, w http.ResponseWriter) {
	switch method {
	case transport.RPCRequestVote:
		var args RequestVoteArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onRequestVote(&args))
	case transport.RPCAppendEntries:
		var args AppendEntriesArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onAppendEntries(&args))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// ------------------------------------------------------------
// *Testing helpers* – read-only accessors use RLock
// ------------------------------------------------------------

func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) LastApplied() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
}

func (n *Node) Log() StableLog { return n.log }

func (n *Node) GetDB() *bolt.DB { return n.db }

func (n *Node) Peers() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}

func (n *Node) PeersCopy() map[string]string {
	out := make(map[string]string, len(n.peers))
	for k, v := range n.peers {
		out[k] = v
	}
	return out
}

func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }
