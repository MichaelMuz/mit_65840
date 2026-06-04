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
	noop  bool
	Value int
}
type PersistentState struct {
	currentTerm int
	votedFor    int
	log         []LogEntry
}

type Raft struct {
	mu        *sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	PersistentState

	commitIndex int
	lastApplied int

	leader      int
	leaderLease time.Time
	state       EState

	nextIndex  []int
	matchIndex []int
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex > lastLogIndex)) {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.leader = -1 // maybe wanna set this if term is bigger even if log isn't good
		reply.VoteGranted = true
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.currentTerm = args.Term
	rf.state = Follower
	rf.votedFor = args.LeaderId // didn't vote but don't hand out vote for this term anymore
	rf.leader = args.LeaderId
	rf.leaderLease = time.Now()

	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
		reply.Success = true
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// this thing knows when we turn to a leader so maybe we should go call the heartbeats here
// then they can return if they notice we are not the leader, but then we would need to cancel
// technically we are supposed to reset timer right after we start election and bail early if chimes
func (rf *Raft) candidateLoop() {
	for {
		dur := time.Duration(50+(rand.Int63()%300)) * time.Millisecond
		time.Sleep(dur)
		rf.mu.Lock()
		if rf.state != Follower || time.Since(rf.leaderLease) < dur {
			rf.mu.Unlock()
			continue
		}

		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me // vote for ourselves this term
		rf.leader = -1

		li := len(rf.log) - 1
		lt := rf.log[li].Term

		args := []RequestVoteArgs{}
		for range rf.others() {
			args = append(args, RequestVoteArgs{rf.currentTerm, rf.me, li, lt})
		}

		peers := len(rf.peers)
		rf.mu.Unlock()

		reps := make(chan *RequestVoteReply, peers-1)
		for i, a := range args {
			go func() {
				r := RequestVoteReply{}
				rf.sendRequestVote(i, &a, &r)
				reps <- &r // if we failed then no vote and term is 0, fine
			}()
		}

		needY := peers/2 + 1
		needN := peers - needY + 1
		votesY := 1 // voted for self
		votesN := 0
		for r := range reps {
			if r.VoteGranted {
				votesY += 1
			} else {
				votesN += 1
			}

			if votesY >= needY {
				break
			} else if votesN >= needN {
				break
			}
		}

		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			continue
		}

		if votesY >= needY {
			for i := range rf.others() {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.state = Leader
			rf.leader = rf.me
			// add noop log so we can commit from prev turn without edge cases
			rf.log = append(rf.log, LogEntry{rf.currentTerm, true, 0})
		} else {
			rf.state = Follower
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) peerHeartBeat(i int) {
	for {
		time.Sleep(200 * time.Millisecond)
		needsMore := true
		for needsMore {
			needsMore = false

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				break
			}
			pi := rf.nextIndex[i] - 1
			a := AppendEntriesArgs{rf.currentTerm, rf.me, pi, rf.log[pi].Term, rf.log[rf.nextIndex[i]:], rf.commitIndex}
			rf.mu.Unlock()

			r := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &a, &r)

			rf.mu.Lock()

			if rf.state != Leader {
			} else if !ok {
				needsMore = true
			} else if r.Success {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = len(rf.log) - 1
				// could have just achieved majority, update commited
				srt := slices.Sorted(slices.Values(rf.matchIndex))
				slices.Reverse(srt)
				// we always add a noop log as soon as we are leader so we know our term is at the top so prev term edge case doesn't exist
				rf.commitIndex = srt[len(rf.peers)/2+1]
			} else if r.Term > rf.currentTerm {
				rf.currentTerm = r.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.leader = -1
			} else {
				rf.nextIndex[i]--
				needsMore = true
			}
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	mu := sync.Mutex{}

	mu.Lock()
	ps := PersistentState{votedFor: -1, log: []LogEntry{{-1, true, -1}}}
	rf := &Raft{mu: &mu, peers: peers, persister: persister, me: me, PersistentState: ps, leaderLease: time.Now(), leader: -1}
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

	return rf.currentTerm, rf.state == Leader
}
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}
