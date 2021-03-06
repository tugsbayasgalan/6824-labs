package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	FOLLOWER  int = 1
	CANDIDATE int = 2
	LEADER    int = 3
	minTime       = 300
	maxTime       = 500
	heartBeat     = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Additional properties in 2A
	state         int
	currentTerm   int
	votedFor      int
	numVotes      int
	electionTimer *time.Timer
	chanVote      chan bool
	chanHeartBeat chan bool

	// Additional properties in 2B
	log         []LogEntry
	commitIndex int
	lastApplied int

	//for leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	//fmt.Println(strconv.Itoa(rf.state))

	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// type for AppendEntries
//

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	if rf.currentTerm > args.Term {

		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.votedFor = -1

	} else {
		if rf.currentTerm < args.Term {

			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.toState(FOLLOWER)
			reply.Term = args.Term

		} else {

			if rf.state == FOLLOWER && rf.votedFor == -1 || rf.votedFor == args.CandidateID {

				if len(rf.log) == 0 || args.LastLogTerm > rf.log[len(rf.log)-1].Term {
					rf.votedFor = args.CandidateID
					reply.VoteGranted = true

				} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
					rf.votedFor = args.CandidateID
					reply.VoteGranted = true
				} else {
					rf.votedFor = -1
					reply.VoteGranted = false
				}

			} else {
				reply.VoteGranted = false
			}

		}

	}

	if reply.VoteGranted {
		go func() {
			rf.chanVote <- true
		}()
	}

	rf.mu.Unlock()

}

//
// Append entries handler
//

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {

	rf.mu.Lock()

	// handle terms
	if rf.currentTerm > args.Term {
		// reply false if arg term is less than current term
		reply.Success = false
		reply.Term = rf.currentTerm

	} else if rf.currentTerm < args.Term {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.toState(FOLLOWER)
	} else {
		reply.Success = true
	}

	// handle logs
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	}

	// handle conficts
	conflictIndex := -1

	if len(rf.log)-1 < args.PrevLogIndex+len(args.Entries) {
		conflictIndex = args.PrevLogIndex + 1
	} else {

		for i := 0; i < len(args.Entries); i++ {
			if rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				conflictIndex = args.PrevLogIndex + i + 1
				break
			}
		}

	}

	// if there was a conflict
	if conflictIndex != -1 {
		for i := 0; i < len(args.Entries); i++ {
			rf.log[args.PrevLogIndex+i+1] = args.Entries[i]
		}

	}

	if args.LeaderCommit > rf.commitIndex {

		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}

	}

	go func() {
		rf.chanHeartBeat <- true
	}()

	rf.mu.Unlock()

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		newLog := LogEntry{Command: command, Term: term}
		index = len(rf.log)
		rf.log = append(rf.log, newLog)

	}

	// Your code here (2B).

	return index, term, isLeader
}

// update the state to given state
func (rf *Raft) toState(state int) {
	//fmt.Println("Getting here???")

	switch state {
	case FOLLOWER:

		rf.state = FOLLOWER
		rf.votedFor = -1

	case CANDIDATE:

		rf.state = CANDIDATE
		rf.startElection()

	case LEADER:
		rf.state = LEADER

	default:
		fmt.Printf("some weird state is happening")

	}

}

// start election
func (rf *Raft) startElection() {
	//rf.mu.Lock()

	//fmt.Println("Getting here to starting election " + strconv.Itoa(rf.me) + " And my term was " + strconv.Itoa(rf.currentTerm))
	rf.currentTerm++
	//fmt.Println("Getting here to starting election " + strconv.Itoa(rf.me) + " And my term is " + strconv.Itoa(rf.currentTerm))
	rf.votedFor = rf.me
	rf.numVotes = 1
	rf.electionTimer.Reset(randomElectionInterval())

	reqArg := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me}

	//rf.mu.Unlock()
	for index := range rf.peers {
		if index != rf.me {

			go func(server int) {
				reply := new(RequestVoteReply)
				if rf.sendRequestVote(server, &reqArg, reply) {
					rf.mu.Lock()

					if reply.VoteGranted {

						rf.numVotes++

						//fmt.Println("num votes is " + strconv.Itoa(rf.numVotes) + "for " + strconv.Itoa(rf.me))

					} else {
						if rf.currentTerm < reply.Term {

							rf.currentTerm = reply.Term
							rf.toState(FOLLOWER)
						}
					}
					rf.mu.Unlock()
				}
			}(index)
		}
	}

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

func (rf *Raft) communicateHeartBeat() {

	appendArg := AppendEntries{Term: rf.currentTerm, LeaderId: rf.me}

	for index := range rf.peers {
		if index != rf.me && rf.state == LEADER {

			go func(server int) {
				reply := new(AppendEntriesReply)

				if rf.sendAppendEntries(server, &appendArg, reply) {

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !reply.Success {

						if rf.currentTerm < reply.Term {
							//fmt.Println("IS it ever false?")

							rf.currentTerm = reply.Term

							rf.toState(FOLLOWER)

						}
					}
				} else {

					//fmt.Println("Something wrong with reply from server " + strconv.Itoa(server) + " for " + strconv.Itoa(rf.me) + " and " + strconv.Itoa(int(rf.state)))
				}
			}(index)
		}
	}

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
	rf.state = FOLLOWER

	rf.votedFor = -1
	rf.chanVote = make(chan bool)
	rf.chanHeartBeat = make(chan bool)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {

		rf.electionTimer = time.NewTimer(randomElectionInterval())

		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case FOLLOWER:
				select {
				case <-rf.chanHeartBeat:
					rf.electionTimer.Reset(randomElectionInterval())
				case <-rf.chanVote:
					rf.electionTimer.Reset(randomElectionInterval())
				case <-rf.electionTimer.C:
					rf.mu.Lock()
					rf.toState(CANDIDATE)
					rf.mu.Unlock()

				}

			case CANDIDATE:

				select {

				case <-rf.chanHeartBeat:
					rf.mu.Lock()
					rf.toState(FOLLOWER)
					rf.mu.Unlock()
				case <-rf.electionTimer.C:
					//fmt.Println("Getting here to start new election " + strconv.Itoa(rf.me))
					rf.electionTimer.Reset(randomElectionInterval())

					rf.startElection()

				default:
					rf.mu.Lock()
					if rf.numVotes > len(rf.peers)/2 {

						//fmt.Println("New leader " + strconv.Itoa(rf.me))

						rf.toState(LEADER)
					}
					rf.mu.Unlock()
				}

			case LEADER:

				select {
				case <-rf.chanHeartBeat:
					rf.mu.Lock()
					rf.toState(FOLLOWER)
					rf.mu.Unlock()
				default:
					rf.mu.Lock()
					rf.communicateHeartBeat()
					time.Sleep(heartBeat * time.Millisecond)
					rf.mu.Unlock()

				}

			}

		}

	}()

	return rf
}

//helper functions are here

func randomElectionInterval() time.Duration {

	return time.Duration(rand.Intn(maxTime-minTime)+minTime) * time.Millisecond

}
