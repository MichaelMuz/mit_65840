package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term  int
	Value int
}
type PersistentState struct {
	currentTerm int
	votedFor    int
	log         []LogEntry
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	electionTimer time.Ticker
	isLeader      bool

	PersistentState

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) reset() {
	rf.electionTimer = *time.NewTicker(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.PersistentState)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

func (rf *Raft) readPersist(data []byte) {
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

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex > lastLogIndex)) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	rf.reset()
	for true {
		<-rf.electionTimer.C

		wg := sync.WaitGroup{}
		for i := range rf.peers {
			wg.Go(func() { rf.sendRequestVote(i, &RequestVoteArgs{}, &RequestVoteReply{}) })
		}
		wg.Wait()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.isLeader = false

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: -1, Value: -1}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for range len(peers) {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
