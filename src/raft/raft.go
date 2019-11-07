package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	log []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       int

	electionTimer *time.Timer

	Candidate2Follower chan int
	Leader2Follower    chan int
	HeartBeatChan      chan int
	commitChan	chan int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2

	HeartBeatsTimer  = time.Duration(50) * time.Millisecond
	ElectionTimerMin = 150
	ElectionTimerMax = 300
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntiresArgs struct {
	Term     int
	LeaderId int
	PrevLogIndex int
	PrevLogeTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	toFollower := false
	reply.VoteGranted = false
	// fmt.Printf("%d,%d\n", args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		fmt.Printf("1Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
		toFollower = true
	}
	reply.Term = rf.currentTerm
	if rf.state == Leader && !toFollower {
		reply.VoteGranted = false
		fmt.Printf("2Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
		return
	}
	if rf.state == Candidate && toFollower {
		rf.Candidate2Follower <- 1
	}
	if rf.state == Leader && toFollower {
		rf.Leader2Follower <- 1
	}

	if (args.LastLogTerm < rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex < rf.log[len(rf.log)-1].Index)){
		reply.VoteGranted = false
		rf.persist()
	}else if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.electionTimer.Reset(randTime())
	}

	fmt.Printf("3Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
}

func (rf *Raft) AppendEntries(args AppendEntiresArgs, reply *AppendEntriesReply) {
	//fmt.Print("in here\n")
	//fmt.Print(args.Entries == nil)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
		return
	}
	if rf.state == Candidate && args.LeaderId != rf.me && args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = Follower
		rf.Candidate2Follower <- 1
		rf.persist()
	}
	if rf.state == Leader && args.LeaderId != rf.me && args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentTerm = -1
		rf.state = Follower
		rf.Leader2Follower <- 1
		rf.persist()
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.electionTimer.Reset(randTime())
	reply.Term = rf.currentTerm

	index := rf.log[len(rf.log)-1].Index

	if args.PrevLogIndex > index {
		reply.NextIndex = index + 1
		return
	}

	indexZero := rf.log[0].Index

	if args.PrevLogIndex > indexZero {
		term := rf.log[args.PrevLogIndex - indexZero].Term
		if args.PrevLogeTerm != term {
			for i:=args.PrevLogIndex-1;i>=indexZero;i--{
				if rf.log[i-indexZero].Term != term{
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex >= indexZero{
		rf.log = rf.log[: args.PrevLogIndex + 1 - indexZero]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		// fmt.Print(rf.log)
		reply.Success = true
		reply.NextIndex = rf.log[len(rf.log)-1].Index
	}

	if args.LeaderCommit > rf.commitIndex{
		last := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit > last{
			rf.commitIndex = last
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitChan <- 1
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state != Leader {
		return index, term, isLeader
	}
	isLeader = true

	index = rf.log[len(rf.log)-1].Index + 1
	entry := Entry{index, term, command}
	rf.log = append(rf.log, entry)
	rf.persist()

	rf.HeartBeatChan <- 1

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Follower
	//rf.voteFor = -1

	rf.Leader2Follower = make(chan int)
	rf.Candidate2Follower = make(chan int)
	rf.HeartBeatChan = make(chan int)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.commitChan = make(chan int)

	rf.electionTimer = time.NewTimer(randTime())

	// initialize from state persisted before a crash
	rf.electionTimer.Reset(randTime())
	rf.log = append(rf.log, Entry{Term: 0})
	rf.readPersist(persister.ReadRaftState())

	go func(){
		for {
			select {
				case <- rf.commitChan:
					index := rf.commitIndex
					for i:=rf.lastApplied+1;i<=index && i<len(rf.log);i++{
						msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
						applyCh <- msg
						rf.lastApplied = i
					}
			}
		}
	}()

	go rf.mainLoop()

	return rf
}

func randTime() time.Duration {
	r := rand.Intn(ElectionTimerMax - ElectionTimerMin)
	return time.Duration(ElectionTimerMin+r) * time.Millisecond
}

func (rf *Raft) mainLoop() {
	for {
		switch state := rf.state; state {
		case Follower:
			for {
				select {
				case <-rf.electionTimer.C:
					rf.currentTerm++
					fmt.Printf("Server %d, Term %d\n", rf.me, rf.currentTerm)
					rf.voteFor = -1
					rf.state = Candidate
					rf.electionTimer.Reset(randTime())
					//return
				}
				if rf.state == Candidate {
					break
				}
			}
		case Candidate:
			func() {
				rf.voteFor = rf.me
				//rf.persist()
				count := 1
				n := len(rf.peers)
				success := make(chan int)
				go func() {
					reply := make(chan *RequestVoteReply)
					for i := 0; i < n; i++ {
						if i != rf.me {
							go func(i int) {
								var tReply *RequestVoteReply
								// fmt.Printf("%d,%d\n", args.CandidateId, args.Term)
								ok := rf.peers[i].Call("Raft.RequestVote", RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}, &tReply)
								if ok {
									//fmt.Printf("get Server %d vote for Server %d - %t in Term %d\n", i, rf.voteFor, tReply.VoteGranted, tReply.Term)
									reply <- tReply
								}
								return
							}(i)
						}
					}
					for {
						// fmt.Print("in count vote\n")
						rep := <-reply
						if rep.Term > rf.currentTerm {
							rf.state = Follower
							rf.voteFor = -1
							rf.currentTerm = rep.Term
							rf.persist()
							rf.Candidate2Follower <- 1
							return
						}
						if rep.VoteGranted {
							count += 1
							// fmt.Printf("In term %d Server %d get a vote, total vote %d\n", rep.Term, rf.voteFor, count)
							if count > n/2 {
								success <- 1
								//rf.state = Leader
								return
							}
						}
					}
				}()
				select {
				case <-success:
					rf.mu.Lock()
					rf.state = Leader
					rf.voteFor = -1
					fmt.Printf("Server %d become Leader in Term %d, state %d\n", rf.me, rf.currentTerm, rf.state)
					rf.persist()
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i:= range rf.peers{
						rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					return
				case <-rf.Candidate2Follower:
					rf.state = Follower
					// rf.voteFor = -1
					fmt.Printf("Server %d become Follower in Term %d\n", rf.me, rf.currentTerm)
					return
				case <-rf.electionTimer.C:
					rf.electionTimer.Reset(randTime())
					rf.currentTerm += 1
					rf.voteFor = -1
					rf.persist()
					return
				}
			}()
		case Leader:
			//fmt.Print("in leader\n")
			func() {
				nPeers := len(rf.peers)
				rf.mu.Lock()
				rf.nextIndex = make([]int, nPeers)
				rf.matchIndex = make([]int, nPeers)
				for i := 0; i < nPeers; i++{
					rf.nextIndex[i] = rf.lastApplied + 1
					rf.matchIndex[i] = 0
				}
				rf.persist()
				rf.mu.Unlock()

				//n := len(rf.peers)
				heartBeat := time.NewTicker(HeartBeatsTimer)
				defer heartBeat.Stop()
				go func() {
					for _ = range heartBeat.C {
						rf.HeartBeatChan <- 1
					}
				}()
				for {
					select {
					case <-rf.HeartBeatChan:
						n := len(rf.peers)
						base := rf.log[0].Index
						commit := rf.commitIndex
						last := rf.log[len(rf.log)-1].Index
						for i:=rf.commitIndex+1;i<=last;i++{
							num := 1
							for j:=range rf.peers{
								if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-base].Term == rf.currentTerm{
									num ++
								}
							}
							if num*2 > len(rf.peers){
								commit = i
							}
						}
						if commit != rf.commitIndex{
							fmt.Println("commit: ", commit)
							rf.commitIndex = commit
							rf.commitChan <- 1
						}
						for i := 0; i < n; i++ {
							if i != rf.me {
								var reply *AppendEntriesReply
								var args AppendEntiresArgs
								args.Term = rf.currentTerm
								args.LeaderId = rf.me
								args.PrevLogIndex = rf.nextIndex[i] - 1
								if args.PrevLogIndex > len(rf.log) - 1{
									args.PrevLogIndex = len(rf.log) - 1
								}
								args.PrevLogeTerm = rf.log[args.PrevLogIndex].Term
								args.LeaderCommit = rf.commitIndex
								if rf.nextIndex[i] > base{
									args.Entries = make([]Entry, len(rf.log[args.PrevLogIndex + 1: ]))
									copy(args.Entries, rf.log[args.PrevLogIndex + 1: ])
								}
								go func(i int, args AppendEntiresArgs) {
									if rf.peers[i].Call("Raft.AppendEntries", args, &reply){
										if reply.Success{
											if len(args.Entries) > 0{
												rf.mu.Lock()
												rf.nextIndex[i] = args.Entries[len(args.Entries) -1].Index + 1
												fmt.Printf("nextIndex %d:%d\n",i,rf.nextIndex[i])
												rf.matchIndex[i] = rf.nextIndex[i] - 1
												rf.mu.Unlock()
												//rf.persist()
											}
										}else{
											rf.nextIndex[i] = reply.NextIndex
											//rf.persist()
										}
									}
								}(i, args)
							}
						}
					case <-rf.Leader2Follower:
						rf.state = Follower
						rf.voteFor = -1
						rf.persist()
						return
					}
				}
			}()
		}
	}
}
