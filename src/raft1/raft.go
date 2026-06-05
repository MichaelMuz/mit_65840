package raft

import (
	"bytes"
	"iter"
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type EState int

const (
	Follower  EState = iota
	Candidate EState = iota
	Leader    EState = iota
)

type LogEntry struct {
	Term  int
	Noop  bool
	Value int
}
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

type Raft struct {
	mu        *sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	PersistentState

	commitIndex int
	lastApplied int

	leader          int
	leaderLease     time.Time
	resetHeartBeats []chan struct{}
	state           EState

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) dbg(format string, a ...any) {
	DPrintf("[%v] "+format, append([]any{rf.me}, a...)...)
}

func (rf *Raft) others() iter.Seq2[int, *labrpc.ClientEnd] {
	return func(yield func(int, *labrpc.ClientEnd) bool) {
		for i, p := range rf.peers {
			if i == rf.me {
				continue
			}
			if !yield(i, p) {
				return
			}
		}
	}
}

// assumed to be called with lock engaged
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.PersistentState)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	// rf.dbg("Persisted state, currentTerm: %v, votedFor: %v \n", rf.CurrentTerm, rf.VotedFor)
}

func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var s PersistentState
	if err := d.Decode(&s); err != nil {
		log.Fatal(err)
	} else {
		rf.PersistentState = s
	}

	rf.dbg("Read persisted state, currentTerm: %v, votedFor: %v \n", rf.CurrentTerm, rf.VotedFor)
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dbg("Incoming RV (my term:%v) by peer %v, with term %v \n", rf.CurrentTerm, args.CandidateId, args.Term)

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm || (args.Term == rf.CurrentTerm && (rf.VotedFor != -1 || rf.VotedFor != args.CandidateId)) {
		rf.dbg("Rejected RV on term: term:%v by peer %v, with term %v, my term: %v, votedFor: %v \n", rf.CurrentTerm, args.CandidateId, args.Term, rf.CurrentTerm, rf.VotedFor)
		return
	}

	rf.leader = -1

	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		rf.CurrentTerm = args.Term
		rf.state = Follower
		rf.VotedFor = args.CandidateId
		rf.leader = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.dbg("Granted RV to peer %v", args.CandidateId)
	} else {
		rf.dbg("Rejected RV on log: (lastTerm:%v, lastIndex:%v) by peer %v, with lastTerm %v, lastIndex: %v\n", lastLogTerm, lastLogIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	rf.dbg("Sending RV from %v, with args: {Term: %v, LastLogIndex: %v, LastLogTerm: %v} \n", server, args.Term, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.dbg("Sent RV from %v, ok=%v \n", server, ok)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dbg("Incoming AE: {term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, len(Entries): %v, LeaderCommit: %v} \n", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)

	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		rf.dbg("Rejected incoming AE on term, our %v > their %v \n", rf.CurrentTerm, args.Term)
		return
	}

	rf.CurrentTerm = args.Term
	rf.state = Follower
	rf.VotedFor = args.LeaderId // didn't vote but don't hand out vote for this term anymore
	rf.leader = args.LeaderId
	rf.leaderLease = time.Now()

	if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
		rf.commitIndex = min(len(rf.Log)-1, args.LeaderCommit)
		reply.Success = true
		rf.persist()
		rf.dbg("Accepted incoming AE from %v, commitIndex: %v, len(log): %v", args.LeaderId, rf.commitIndex, len(rf.Log))
	} else {
		rf.dbg("Temporarily rejected incoming AE on log, ours: (len(log): %v), theirs (PrevLogIndex: %v, PrevLogTerm: %v) \n", len(rf.Log), args.PrevLogIndex, args.PrevLogTerm)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// rf.dbg("Sending AE to %v: {term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, len(Entries): %v, LeaderCommit: %v} \n", server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// rf.dbg("Sending AE to %v, ok=%v \n", server, ok)
	return ok
}

// this thing knows when we turn to a leader so maybe we should go call the heartbeats here
// then they can return if they notice we are not the leader, but then we would need to cancel
// technically we are supposed to reset timer right after we start election and bail early if chimes
func (rf *Raft) candidateLoop() {
	for {
		dur := time.Duration(300+(rand.Int63()%150)) * time.Millisecond
		time.Sleep(dur)
		rf.mu.Lock()
		if rf.state != Follower || time.Since(rf.leaderLease) < dur {
			rf.mu.Unlock()
			continue
		}

		rf.dbg("Becoming candidate \n")

		rf.state = Candidate
		rf.CurrentTerm++
		rf.VotedFor = rf.me // vote for ourselves this term
		rf.leader = -1

		li := len(rf.Log) - 1
		lt := rf.Log[li].Term
		peers := len(rf.peers)

		fs := []func(){}
		reps := make(chan *RequestVoteReply, peers-1)
		for i := range rf.others() {
			a := &RequestVoteArgs{rf.CurrentTerm, rf.me, li, lt}
			fs = append(fs, func() {
				r := RequestVoteReply{}
				_ = rf.sendRequestVote(i, a, &r)
				reps <- &r // if we failed then no vote and term is 0, fine
			})
		}

		term := rf.CurrentTerm
		maxTerm := rf.CurrentTerm

		rf.mu.Unlock()

		for _, f := range fs {
			go f()
		}

		needY := peers/2 + 1
		needN := peers - needY + 1
		votesY := 1 // voted for self
		votesN := 0
		for r := range reps {
			if r.VoteGranted {
				votesY += 1
			} else if r.Term > term {
				// immediately step down to follower
				maxTerm = r.Term
				break
			} else {
				votesN += 1
			}

			if votesY >= needY {
				break
			} else if votesN >= needN {
				break
			}
		}

		rf.dbg("Got %v yes, %v no\n", votesY, votesN)

		rf.mu.Lock()
		if maxTerm > term {
			if maxTerm > term {
				rf.dbg("Stepped down bc someone had term %v which is bigger than mine: %v", maxTerm, rf.CurrentTerm)
			}
			rf.CurrentTerm = max(rf.CurrentTerm, maxTerm)
			rf.state = Follower
			rf.VotedFor = -1 // idk who the leader is
			rf.leader = -1
			rf.mu.Unlock()
			continue
		}
		if rf.state != Candidate {
			rf.mu.Unlock()
			continue
		}

		if votesY >= needY {
			rf.dbg("Won election \n")
			for i := range rf.others() {
				rf.nextIndex[i] = len(rf.Log)
				rf.matchIndex[i] = 0
			}
			rf.state = Leader
			rf.leader = rf.me
			// add noop log so we can commit from prev turn without edge cases
			rf.Log = append(rf.Log, LogEntry{rf.CurrentTerm, true, 0})
			for i := range len(rf.peers) {
				select {
				// send heartbeat immediately
				// don't need this buffered bc if we are currently sending a heartbeat no need for another
				case rf.resetHeartBeats[i] <- struct{}{}:
				default:
				}
			}
		} else {
			rf.dbg("Stepping down to follower from candidate\n")
			rf.state = Follower
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) peerHeartBeat(i int) {
	// timer := time.NewTimer(d time.Duration)
	for {
		select {
		case <-time.After(200 * time.Millisecond):
			// rf.dbg("Wokeup heartbeat to peer %v based on timer", i)
		case <-rf.resetHeartBeats[i]:
			// rf.dbg("Wokeup heartbeat to peer %v based on reset", i)
		}

		needsMore := true
		for needsMore {
			needsMore = false

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				break
			}

			pi := rf.nextIndex[i] - 1
			a := AppendEntriesArgs{rf.CurrentTerm, rf.me, pi, rf.Log[pi].Term, rf.Log[rf.nextIndex[i]:], rf.commitIndex}
			rf.mu.Unlock()

			r := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &a, &r)

			rf.mu.Lock()

			if rf.state != Leader {
			} else if !ok {
				needsMore = true
			} else if r.Success {
				rf.nextIndex[i] = len(rf.Log)
				rf.matchIndex[i] = len(rf.Log) - 1
				// could have just achieved majority, update commited
				srt := slices.Sorted(slices.Values(rf.matchIndex))
				slices.Reverse(srt)
				// we always add a noop log as soon as we are leader so we know our term is at the top so prev term edge case doesn't exist
				rf.commitIndex = srt[len(rf.peers)/2+1]
				rf.dbg("Peer %v caught up on log \n", i)
			} else if r.Term > rf.CurrentTerm {
				rf.CurrentTerm = r.Term
				rf.state = Follower
				rf.VotedFor = -1
				rf.leader = -1
				rf.dbg("Peer %v knows about term %v, stepping down \n", i, r.Term)
			} else {
				rf.nextIndex[i]--
				needsMore = true
				rf.dbg("Peer %v not caught up, retrying heartbeat with prev ind \n", i)
			}
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	mu := sync.Mutex{}

	mu.Lock()
	ps := PersistentState{VotedFor: -1, Log: []LogEntry{{-1, true, -1}}}

	rf := &Raft{mu: &mu, peers: peers, persister: persister, me: me, PersistentState: ps, leaderLease: time.Now(), leader: -1, nextIndex: make([]int, len(peers)), matchIndex: make([]int, len(peers)), resetHeartBeats: make([]chan struct{}, len(peers))}

	rf.dbg("Make called on me \n")

	go rf.candidateLoop()
	for i := range rf.others() {
		go rf.peerHeartBeat(i)
	}
	mu.Unlock()

	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dbg("Being tested, I am leader: %v, my current term: %v", rf.state == Leader, rf.CurrentTerm)

	return rf.CurrentTerm, rf.state == Leader
}
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}
