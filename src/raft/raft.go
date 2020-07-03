package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
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
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
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
	// 需要被持久化的
	currentTerm int // 当前任期号
	voteForTerm int // 当前投票给哪个任期
	//votedFor    int        // 在当前获得选票的候选人的 Id
	log []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
	// volatile的
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	// leader volatile(选举后重新初始化)
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值
	// custom
	random *rand.Rand // 随机生成类
	// 收到 AppendEnrties 或 HeartBeat 则通知
	// headbeatCh chan struct{}{};
	// 节点状态
	state int
	// 选举定时器
	electionTimer *time.Timer
	// 判断选举是否完成
	//electionDone bool
	// 心跳定时器
	heartbeatTimer []*time.Timer
	// 仅初始化一次心跳定时器
	//initHeartbeatTimerOnce sync.Once
	// 是否关闭
	isKilled bool
	// apply channel
	applyCh chan ApplyMsg
}

type LogEntry struct {
	// 指令
	Command interface{}
	// 任期号
	Term int
	// 索引
	Index int
}

const HeartBeatDuration = time.Duration(time.Millisecond * 50)
//const HeartBeatDuration = time.Duration(time.Millisecond * 600)
const ApplyDuration = time.Duration(time.Millisecond * 20)

const CandidateDurationMin = time.Duration(time.Millisecond * 250)
//const CandidateDurationMin = time.Duration(time.Millisecond * 1200)

const Leader, Follower, Candidate int = 1, 2, 3

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	//DPrintf("持久化")
	//rf.mu.Lock()
	currentTerm := rf.currentTerm
	voteFor := rf.voteForTerm
	logEntries := rf.log
	//rf.mu.Unlock()

	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)

	encoder.Encode(currentTerm)
	encoder.Encode(voteFor)

	//if rf.lastApplied != 0 {
	//	arrayIndex := rf.getArrayIndex(rf.lastApplied) + 1
	//	if arrayIndex == len(rf.log) {
	encoder.Encode(logEntries)
	//}else {
	//	encoder.Encode(logEntries[:arrayIndex])
	//}
	//}

	data := byteBuffer.Bytes()

	rf.persister.SaveRaftState(data)
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
	DPrintf("读取持久化信息")
	byteBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(byteBuffer)

	var (
		currentTerm int
		voteFor     int
		logEntries  []LogEntry
	)

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&voteFor) != nil ||
		decoder.Decode(&logEntries) != nil {
		fmt.Println("error for persisting...")
	} else {
		//rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.voteForTerm = voteFor
		rf.log = logEntries
		//rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人ID
	LastLogIndex int // 候选人最后一个日志的索引值
	LastLogTerm  int // 候选人最后一个日志的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 是否赢得这张选票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]:term=%d 收到[%d]的投票请求 args: [%d], 我的voteForTer:%d, state:%d",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.voteForTerm, rf.state)
	// 如果term < currentTerm返回 false （5.2 节）
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	defer rf.persist()
	// if args.LastLogTerm < rf.
	// 如果 votedFor 为空或者为 CandidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	// 请求选票节点的 Term 必须大于已经投票过的 Term
	if rf.voteForTerm >= args.Term {
		DPrintf("voteForTerm显示已经投过票了，拒绝%d的投票请求", args.CandidateId)
		reply.VoteGranted = false
		return
	}

	currentLastLogTerm, currentLastLogIndex := rf.getLogInfo()
	DPrintf("me:%d, currentT:%d, currentIndex:%d, argsT:%d, argsIndex:%d",
		rf.me, currentLastLogTerm, currentLastLogIndex, args.LastLogTerm, args.LastLogIndex)
	// 当前 log 的 Term 比请求节点高，不予投票
	if currentLastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		DPrintf("[%d] 当前log的Term比较高，比较新不予投票", rf.me)
		return
	}
	// 当前 log 的 Term 和请求节点一样，比较长度，如果当前长度比请求节点长，不予投票
	if currentLastLogTerm == args.LastLogTerm && currentLastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		DPrintf("[%d] 当前log的Term一样，但是index没当前高，比较新不予投票", rf.me)
		return
	}
	// 到这里有两种可能性
	// 1. 当前log 的 Term 比请求节点低
	// 2. 当前log 的 Term 和请求节点相同，但当前长度小于等于请求节点，也就是至少和自己一样新
	// 投票给请求节点
	//rf.votedFor = args.CandidateId
	rf.voteForTerm = args.Term
	reply.VoteGranted = true
	DPrintf("[%d] 投票给 %d", rf.me, args.CandidateId)
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	if rf.state != Follower {
		//DPrintf("%d 收到投票请求成为 follower", rf.me)
		rf.toFollower()
	}
	//DPrintf("[%d] 投票给 %d, and reset timer", rf.me, args.CandidateId)
	rf.resetTimer()
}

type AppendEntriesArgs struct {
	Term         int        // 领导人的任期号
	LeaderId     int        // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term         int  // 当前的任期号，用于领导人去更新自己
	Success      bool // 跟随者包含了匹配上 PrevLogIndex 和 PrevLogTerm 的日志时为真
	EarlierTerm  int  // 匹配失败，需要根据此参数匹配最早相同的那个日志
	EarlierIndex int  // 匹配失败，需要根据此参数匹配最早相同的那个日志
	//FullLogAppend bool // 全量复制
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 如果 Term < currentTerm 就返回 false （5.1 节）
	if args.Term < rf.currentTerm {
		DPrintf("[%d] AE收到[%d]的心跳,但term太低,返回false", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.mu.Lock()
	if rf.state == Candidate || rf.state == Leader {
		//DPrintf("%d 收到心跳请求, 成为follower", rf.me)
		rf.toFollower()
	}
	rf.currentTerm = args.Term
	defer rf.mu.Unlock()
	defer rf.resetTimer()
	//defer DPrintf("[%d] 收到appendEntries请求 reset timer", rf.me)
	// 心跳请求
	if args.Entries == nil && args.PrevLogIndex == -1 {
		DPrintf("[%d] state:[%d] 收到心跳, currentTerm从%d变为%d", rf.me, rf.state, rf.currentTerm, args.Term)
		rf.persist()
		// 如果 LeaderCommit > commitIndex，令 commitIndex 等于 LeaderCommit 和 新日志条目索引值中较小的一个
		if args.LeaderCommit != -1 {
			if rf.commitIndex < args.LeaderCommit {
				//DPrintf("me:%d, commitIndex from %d to %d", rf.me, rf.commitIndex, args.LeaderCommit)
				rf.commitIndex = args.LeaderCommit
			}
		}

		reply.Term = args.Term
		reply.Success = true
		return
	}

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	if prevLogIndex == -1 {
		// 全量直接复制
		rf.log = args.Entries
		// 如果 LeaderCommit > commitIndex，令 commitIndex 等于 LeaderCommit 和 新日志条目索引值中较小的一个
		if args.LeaderCommit != -1 {
			if rf.commitIndex < args.LeaderCommit {
				if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
					//DPrintf("4) me:%d, commitIndex from %d to %d", rf.me, rf.commitIndex, args.LeaderCommit)
					rf.commitIndex = args.LeaderCommit
				} else {
					//DPrintf("5) me:%d, commitIndex from %d to %d", rf.me, rf.commitIndex, rf.log[len(rf.log)-1].Index)
					rf.commitIndex = rf.log[len(rf.log)-1].Index
				}
			}
		}
		DPrintf("全量复制，[%d]与leader同步成功", rf.me)
		reply.Success = true
		return
	}

	// 如果日志在 PrevLogIndex 位置处的日志条目的任期号和 PrevLogTerm 不匹配，则返回 false （5.3 节）
	lastLog, ok := rf.getLastLog()
	// 日志无内容，直接从头开始
	if !ok {
		DPrintf("日志无内容, [%d] 要求leader从头开始", rf.me)
		reply.Success = false
		reply.EarlierTerm = -1
		reply.EarlierIndex = -1
		return
	}

	// 日志短了
	if lastLog.Index < prevLogIndex {
		DPrintf("日志短了, [%d] ", rf.me)
		rf.replyLessThanLeaderIndex(prevLogTerm, lastLog, reply)
		return
	}

	arrayIndex := rf.getArrayIndex(prevLogIndex)
	checkLog := rf.log[arrayIndex]

	// 不匹配
	if checkLog.Term != prevLogTerm {

		// 第一个就不匹配 直接全量
		if arrayIndex == 0 {
			reply.Success = false
			reply.EarlierTerm = -1
			reply.EarlierIndex = -1
			return
		}
		// 给的Index对应的log term比我大，返回我的让leader找第一个比我小于等于的log
		if checkLog.Term < prevLogTerm {
			reply.Success = false
			reply.EarlierTerm = checkLog.Term
			reply.EarlierIndex = checkLog.Index
			return
		} else {
			DPrintf("leader给的log term比我小")
			for i := arrayIndex - 1; i >= 0; i-- {

				logEntry := rf.log[i]

				if logEntry.Term <= prevLogTerm {
					reply.Success = false
					reply.EarlierTerm = logEntry.Term
					reply.EarlierIndex = logEntry.Index
					return
				}
			}
			// 没找到,从头开始
			reply.Success = false
			reply.EarlierTerm = -1
			reply.EarlierIndex = -1
			return
		}
	}

	// 匹配
	// 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的附加日志中尚未存在的任何新条目 （5.3 节）
	appendLogArray := args.Entries
	expectLength := arrayIndex + 1 + len(appendLogArray)
	appendIndex := 0

	if expectLength >= len(rf.log) {

		// 覆盖
		for i := arrayIndex + 1; i < len(rf.log); i++ {
			rf.log[i] = appendLogArray[appendIndex]
			appendIndex++
		}
		// 还没覆盖完
		if appendIndex != len(appendLogArray) {
			for j := appendIndex; j < len(appendLogArray); j++ {
				//DPrintf("%d -> append: %+v", rf.me, appendLogArray[j])
				rf.log = append(rf.log, appendLogArray[j])
			}
		}
	} else {
		//DPrintf("appendLog[]:%+v, arrayIndex:%d, log[]:%+v", appendLogArray, arrayIndex, rf.log)
		// 不仅覆盖，还需要删除后面的log
		end := len(appendLogArray) + arrayIndex
		for i := arrayIndex + 1; i <= end; i++ {
			//DPrintf("append -> log[%d] from %+v to %+v", i, rf.log[i], appendLogArray[appendIndex])
			rf.log[i] = appendLogArray[appendIndex]
			appendIndex++
		}
		//DPrintf("还未删除前 log[]:%+v", rf.log)
		rf.log = rf.log[:expectLength]
		//DPrintf("删除后 log[]:%+v", rf.log)
	}

	// 如果 LeaderCommit > commitIndex，令 commitIndex 等于 LeaderCommit 和 新日志条目索引值中较小的一个
	if args.LeaderCommit != -1 {
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
				//DPrintf("2) me:%d, commitIndex from %d to %d", rf.me, rf.commitIndex, args.LeaderCommit)
				rf.commitIndex = args.LeaderCommit
			} else {
				//DPrintf("3) me:%d, commitIndex from %d to %d, log[]:%+v, checkTerm:%d, prevTerm:%d ",
				//rf.me, rf.commitIndex, rf.log[len(rf.log)-1].Index, rf.log, checkLog.Term, prevLogTerm)
				rf.commitIndex = rf.log[len(rf.log)-1].Index
			}
		}
	}
	rf.apply()
	reply.Success = true
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
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartBeatDuration):
		//rpc调用超时
	}
	return ok
	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 400):
	}
	return ok
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()

	// 如果不是leader 直接返回false
	if !isLeader {
		isLeader = false
		return index, -1, isLeader
	}

	index, term = rf.appendLog(command)
	rf.mu.Lock()
	rf.ReplicationNow()
	rf.mu.Unlock()
	//rf.mu.Lock()
	//rf.persist()
	//rf.mu.Unlock()
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
	rf.mu.Lock()
	DPrintf("%d be killed", rf.me)
	rf.isKilled = true
	rf.electionTimer.Reset(0)
	rf.ReplicationNow()
	rf.mu.Unlock()
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化属性
	rf.currentTerm = 0
	//rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 随机数生成
	rf.random = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.state = Follower
	DPrintf("make [%d]", me)

	// 初始化定时器
	rf.electionTimer = time.NewTimer(CandidateDurationMin)
	rf.heartbeatTimer = make([]*time.Timer, len(rf.peers))
	rf.initReplicationTimer()
	rf.apply()
	rf.mu.Unlock()
	go rf.ElectionLoop()
	//go rf.ApplyLoop()
	go rf.FollowerApply()
	//go rf.LogLoop()
	//go func(rf *Raft) {
	//	timer := time.NewTimer(time.Millisecond * 2000)
	//	for {
	//		<-timer.C
	//		rf.mu.Lock()
	//		DPrintf("%d 是 %d", rf.me, rf.state)
	//		timer.Reset(time.Millisecond * 2000)
	//		rf.mu.Unlock()
	//	}
	//}(rf)
	return rf
}

func (rf *Raft) ElectionLoop() {
	//DPrintf("[%d] 初始化 electionLoop and reset timer", rf.me)
	rf.resetTimer()
	defer rf.electionTimer.Stop()

	for !rf.isKilled {

		<-rf.electionTimer.C

		if rf.isKilled {
			break
		}

		rf.mu.Lock()
		state := rf.state
		currentTerm := rf.currentTerm
		//DPrintf("[%d] state:%d, electionLoop中 loop reset timer", rf.me, rf.state)
		rf.resetTimer()
		rf.mu.Unlock()

		if state == Follower {
			rf.state = Candidate
			// 发起投票
			DPrintf("%d 选举心跳超时,term=%d, 发起选举,state=%d", rf.me, currentTerm, state)
			rf.Vote()
		} else if state == Candidate {
			// 发起投票
			DPrintf("%d 选举心跳超时, 发起选举,state=%d", rf.me, state)
			rf.Vote()
		}
	}
}

func (rf *Raft) resetTimer() {
	// CandidateDurationMin = 150 -> 150-300
	// 200-450
	randomVal := rf.random.Intn(200)
	randomTime := time.Duration(randomVal)*time.Millisecond + CandidateDurationMin
	rf.electionTimer.Reset(time.Duration(randomTime))
}

func (rf *Raft) Vote() {

	rf.mu.Lock()
	// rf.
	peersCount := len(rf.peers)
	var (
		wg sync.WaitGroup
	)
	term := rf.currentTerm + 1
	// 已经投过票了
	if rf.voteForTerm >= term {
		DPrintf("%d term=%d 已经投过票,return", rf.me, term)
		rf.mu.Unlock()
		return
	}
	//rf.votedFor = rf.me
	rf.voteForTerm = term
	rf.currentTerm = term
	rf.persist()

	lastLogTermArgs, lastLogIndexArgs := rf.getLogInfo()
	rf.mu.Unlock()

	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTermArgs,
		LastLogIndex: lastLogIndexArgs,
	}

	voteGrantedCounter := 0
	completeCounter := 0

	wg.Add(1)

	for i := 0; i < peersCount; i++ {

		go func(server int) {

			reply := new(RequestVoteReply)

			if server == rf.me {
				voteGrantedCounter, completeCounter =
					rf.DoneIfFinish(&wg, true, voteGrantedCounter, completeCounter)
				return
			}
			//DPrintf("[%d] 发送RPC", rf.me)
			if success := rf.sendRequestVote(server, &request, reply); !success {
				DPrintf("[%d] 发送给[%d]的RPC失败", rf.me, server)
				voteGrantedCounter, completeCounter =
					rf.DoneIfFinish(&wg, false, voteGrantedCounter, completeCounter)
				return
			}

			// 拿到选票
			if reply.VoteGranted {
				DPrintf("%d 拿到了 %d 的选票", rf.me, server)
				voteGrantedCounter, completeCounter =
					rf.DoneIfFinish(&wg, true, voteGrantedCounter, completeCounter)
				return
			} else {
				// 没拿到选票，如果当前term落后于请求peer，更新term
				DPrintf("[%d] 拿不到 %d 的选票", rf.me, server)
				voteGrantedCounter, completeCounter =
					rf.DoneIfFinish(&wg, false, voteGrantedCounter, completeCounter)
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					if rf.state != Follower {
						//DPrintf("%d 投票的时候别的节点告诉自己太旧了, 变为follower", rf.me)
						rf.toFollower()
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("发起投票rpc结束，voteGrantedCounter: %d, completeCounter: %d", voteGrantedCounter, completeCounter)
	//rf.electionDone = false
	// 赢得选举
	if voteGrantedCounter*2 > peersCount {
		DPrintf("[%d] 赢得选举, term=%d", rf.me, rf.currentTerm)
		rf.toLeader()
		//rf.initHeartbeatTimerOnce.Do(func() {
		//	DPrintf("[%d] 赢得选举,初始化replication timer", rf.me)
		//	rf.initReplicationTimer()
		//})
		return
	}

	// term落后，转为follower等待leader心跳
	if term < rf.currentTerm {
		//DPrintf("%d 投票的时候别的节点告诉自己太旧了2, 变为follower", rf.me)
		rf.toFollower()
	}
}

func (rf *Raft) DoneIfFinish(wg *sync.WaitGroup, voteGranted bool, voteGrantedCounter int, completeCounter int) (int, int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if voteGranted {
		voteGrantedCounter++
	}
	completeCounter++

	peersCount := len(rf.peers)

	if completeCounter == peersCount || voteGrantedCounter*2 > peersCount {

		//if !rf.electionDone {
		defer func() {
			if err := recover(); err != nil {
				//DPrintf("ignore err: %+v", err)
			}
		}()
		//DPrintf("[%d] 选举完成 %d,%d", rf.me, completeCounter, voteGrantedCounter)
		//rf.electionDone = true
		wg.Done()
		//}
	}

	return voteGrantedCounter, completeCounter
}

// return -> lastLogTermArgs, lastLogIndexArgs
func (rf *Raft) getLogInfo() (int, int) {

	if rf.log == nil {
		return 1, 0
	}
	size := len(rf.log)
	if size == 0 {
		return 1, 0
	}

	return rf.log[size-1].Term, rf.log[size-1].Index
}

//func (rf *Raft) AddTerm() int {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.currentTerm++
//	return rf.currentTerm
//}

func (rf *Raft) initReplicationTimer() {

	for i := 0; i < len(rf.heartbeatTimer); i++ {

		if i == rf.me {
			continue
		}

		rf.heartbeatTimer[i] = time.NewTimer(HeartBeatDuration)

		go func(server int) {

			timer := rf.heartbeatTimer[server]
			defer timer.Stop()

			for !rf.isKilled {

				<-timer.C

				if rf.isKilled {
					break
				}

				//rf.mu.Lock()
				////DPrintf("me:[%d],state:[%d] 开始尝试复制 ", rf.me, rf.state)
				//state := rf.state
				//// if Success = false ?
				//// if reply = false ?
				//rf.mu.Unlock()
				_, isLeader := rf.GetState()

				if isLeader {
					//DPrintf("me: %d", rf.me)
					//fmt.Println(rf.nextIndex)

					args, reply, endIndex := rf.prepareRPC(server)
					//DPrintf("%d 发送心跳给 %d server", rf.me, server)

					for {
						//retry := true
						if ok := rf.sendAppendEntries(server, args, reply); ok {

							if !reply.Success {
								// 落后了term,不再是leader
								if reply.Term != -1 {
									rf.mu.Lock()
									//DPrintf("%d 向别人发送心跳时变为 follower", rf.me)
									rf.toFollower()
									rf.currentTerm = reply.Term
									rf.persist()
									rf.mu.Unlock()
									break
								}
								// 日志落后，需要重新调整
								DPrintf("%d的日志落后,重新调整",server)
								rf.checkLog(reply.EarlierIndex, reply.EarlierTerm, server)
								args, reply, endIndex = rf.prepareRPC(server)
							} else {
								// 成功复制日志,更新index
								rf.updateIndex(server, endIndex)
								rf.ApplyLoop()
								break
							}
						} else {
							//DPrintf("%d 发送心跳失败", rf.me)
							//// retry once
							//if retry {
							//	retry = false
							//}else {
							//	break
							//}
							time.Sleep(20 * time.Millisecond)
						}
					}
				}
				rf.mu.Lock()
				timer.Reset(HeartBeatDuration)
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) ReplicationNow() {
	for i := 0; i < len(rf.heartbeatTimer); i++ {
		if i != rf.me {
			rf.heartbeatTimer[i].Reset(0)
		}
	}
}

func (rf *Raft) toFollower() {
	//DPrintf("%d 变为 follower", rf.me)
	//DPrintf("[%d] from %d 变为follower, and reset timer", rf.me, rf.state)
	rf.state = Follower

	rf.resetTimer()
}

// return: index, term
func (rf *Raft) appendLog(command interface{}) (int, int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var (
		term  int
		index int
	)

	size := len(rf.log)
	// 空日志
	if size == 0 {
		index = 1
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	term = rf.currentTerm

	logEntry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	//DPrintf("%d append log", rf.me)
	rf.log = append(rf.log, logEntry)

	return index, term
}

func (rf *Raft) toLeader() {

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	var nextIndex int
	if lastLog, ok := rf.getLastLog(); ok {
		nextIndex = lastLog.Index + 1
	} else {
		nextIndex = -1
	}

	for i := 0; i < len(rf.nextIndex); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = nextIndex
	}

	rf.ReplicationNow()
}

func (rf *Raft) getLastLog() (*LogEntry, bool) {
	logLength := len(rf.log)
	if logLength == 0 {
		return &LogEntry{}, false
	}
	return &rf.log[logLength-1], true
}

func (rf *Raft) prepareRPC(server int) (*AppendEntriesArgs, *AppendEntriesReply, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var (
		prevLogIndex = -1
		prevLogTerm  = -1
		entries      []LogEntry
		leaderCommit = -1
		endIndex     = -1
	)

	nextIndex := rf.nextIndex[server]
	matchIndex := rf.matchIndex[server]

	lastLog, ok := rf.getLastLog()
	// 有日志
	if ok {
		lastIndex := lastLog.Index
		leaderCommit = rf.commitIndex

		// 有日志
		// 已经复制过了，发送心跳
		if lastIndex == matchIndex {
			//DPrintf("发送给 %d 的心跳", server)
			entries = nil
		} else {

			logLength := len(rf.log)
			start := logLength - lastIndex + nextIndex - 1
			//DPrintf("prepareRPC -> start: %d, nextIndex: %d, server:%d, me:%d", start, nextIndex, server, rf.me)

			if start == 0 || start == -1 {
				// 全量
				prevLogIndex = -1
				prevLogTerm = -1

				entries = rf.log
			} else {
				log := rf.log[start-1]
				prevLogIndex = log.Index
				prevLogTerm = log.Term

				for ; start < logLength; start++ {
					entries = append(entries, rf.log[start])
				}
			}

			// 指明复制到哪个Index了
			endIndex = lastIndex
		}
	} else {
		// 没日志
		entries = nil
	}
	//DPrintf("发送给 %d entry[]:%+v, leaderCommit:%d, nextIndex:%d, prevLogIndex:%d, prevTerm:%d",
	//	server, entries, leaderCommit, nextIndex, prevLogIndex, prevLogTerm)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	//fullAppend := prevLogIndex == rf.log[0].Index

	reply := AppendEntriesReply{
		Term:    -1,
		Success: false,
		//FullLogAppend: fullAppend,
	}

	return &args, &reply, endIndex
}

func (rf *Raft) checkLog(index int, term int, server int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("checkLog")
	if index == -1 {
		// 从头开始
		DPrintf("%d 从头开始", server)
		rf.nextIndex[server] = rf.log[0].Index
		return
	}

	lastLog, _ := rf.getLastLog()

	lastIndex := lastLog.Index
	logLength := len(rf.log) - 1

	// 计算出log数组中的数组索引
	arrayIndex := logLength - (lastIndex - index)

	if arrayIndex == 0 {
		// 从头开始
		rf.nextIndex[server] = rf.log[0].Index
		return
	}

	// 此处的Index对应的Log正好相等
	if rf.log[arrayIndex].Term == term {
		DPrintf("logIndex:%d 对应 arrayIndex:%d, Term:%d 匹配成功", index, arrayIndex, term)
		rf.nextIndex[server] = index + 1
	} else {
		// 此处不相等，寻找比follower正好小的log
		var logEntry LogEntry
		found := false
		for i := arrayIndex - 1; i >= 0; i-- {

			logEntry = rf.log[i]

			if logEntry.Term <= term {
				rf.nextIndex[server] = logEntry.Index + 1
				found = true
				DPrintf("找到比follower正好小的log, follower的term=%d, 找到的为%d", term, logEntry.Term)
				break
			}
		}
		if !found {
			// 从头开始
			DPrintf("leader没有找到比follower正好小的log,从头开始")
			rf.nextIndex[server] = rf.log[0].Index
		}
	}
}

func (rf *Raft) updateIndex(server int, endIndex int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("updateIndex: server:%d, endIndex:%d", server, endIndex)
	rf.nextIndex[server] = endIndex + 1
	rf.matchIndex[server] = endIndex
}

func (rf *Raft) ApplyLoop() {

	//timer := time.NewTimer(ApplyDuration)

	//
	//for !rf.isKilled {
	//
	//	<-timer.C
	//
	//	if rf.isKilled {
	//		break
	//	}
	//
	//	_, isLeader := rf.GetState()

	//rf.mu.Lock()
	//DPrintf("%d state:%d 的 logEntries: %+v, commitIndex = %d, lastApplied = %d",
	//	rf.me, rf.state, rf.log, rf.commitIndex, rf.lastApplied)
	//rf.mu.Unlock()

	//if isLeader {

	rf.mu.Lock()
	peersCount := len(rf.peers)
	var (
		maxUpdateIndex = -1
		count          = 2
		commitIndex    = -1
	)

	for i := 0; i < peersCount; i++ {
		if i == rf.me {
			continue
		}

		count = 2
		commitIndex = rf.matchIndex[i]

		if commitIndex <= maxUpdateIndex {
			continue
		}

		for j := 0; j < peersCount; j++ {
			if j == rf.me || j == i {
				continue
			}

			if commitIndex == rf.matchIndex[j] {
				count++
			}
		}
		DPrintf("[%d] check [%d] commitIndex:%d, count:%d", rf.me, i, commitIndex, count)

		if count*2 > peersCount {
			maxUpdateIndex = commitIndex
		}
	}
	//DPrintf("maxIndex: %d", maxUpdateIndex)
	if maxUpdateIndex != -1 {

		if rf.commitIndex < maxUpdateIndex {

			arrayIndex := rf.getArrayIndex(maxUpdateIndex)
			if rf.log[arrayIndex].Term == rf.currentTerm {
				DPrintf("leader apply, commitIndex from %d to %d", rf.commitIndex, maxUpdateIndex)
				rf.commitIndex = maxUpdateIndex
			}
		}
	}

	rf.apply()
	rf.mu.Unlock()
	//}
	//rf.mu.Lock()
	//timer.Reset(HeartBeatDuration)
	//rf.mu.Unlock()
	//}
}

func (rf *Raft) apply() {

	if rf.lastApplied < rf.commitIndex {
		//DPrintf("apply, lastApplied: %d, commitIndex: %d", rf.lastApplied, rf.commitIndex)
		applyCh := rf.applyCh

		var (
			start         int
			end           int
			applyLogIndex int
		)
		if rf.lastApplied == 0 {
			// 还没apply过
			start = 0
		} else {
			start = rf.getArrayIndex(rf.lastApplied)
		}

		lastLogEntry, _ := rf.getLastLog()

		// leader 给的commitIndex太大，只apply到已有的log
		if rf.commitIndex > lastLogEntry.Index {
			applyLogIndex = lastLogEntry.Index
		} else {
			applyLogIndex = rf.commitIndex
		}
		end = rf.getArrayIndex(applyLogIndex)

		//DPrintf("apply-> start:%d, end:%d, length:%d, me:%d, log[]:%+v, lastApply:%d, commitIndex:%d",
		//	start, end, len(rf.log), rf.me, rf.log, rf.lastApplied, rf.commitIndex)

		for ; start <= end; start++ {
			logEntry := rf.log[start]
			//if logEntry.Term == rf.currentTerm {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
			DPrintf("[%d] apply: %+v", rf.me, msg)
			applyCh <- msg
			//}
		}

		rf.lastApplied = applyLogIndex
		// 持久化
		rf.persist()
	}
}

func (rf *Raft) getArrayIndex(logIndex int) int {
	logLength := len(rf.log)
	return logLength - rf.log[logLength-1].Index + logIndex - 1
}

func (rf *Raft) replyLessThanLeaderIndex(preTerm int, logEntry *LogEntry, reply *AppendEntriesReply) {

	reply.Success = false
	if preTerm >= logEntry.Term {
		DPrintf("日志短了, [%d] 要求寻找Index=%d的log", rf.me, logEntry.Index)
		reply.EarlierTerm = logEntry.Term
		reply.EarlierIndex = logEntry.Index
		return
	}

	// 日志term比leader还大，找到比leader正好小于等于的第一个log
	start := rf.getArrayIndex(logEntry.Index)

	if start == 0 {
		DPrintf("日志短了, [%d] 要求leader给全量", rf.me)
		reply.EarlierTerm = -1
		reply.EarlierIndex = -1
		return
	}
	start--
	for ; start >= 0; start-- {
		// 找到第一个至少比leader给的term小或等于的log
		if rf.log[start].Term <= preTerm {
			reply.EarlierTerm = rf.log[start].Term
			reply.EarlierIndex = rf.log[start].Term
			DPrintf("找到了, [%d] 要求寻找Index=%d的log", rf.me, logEntry.Index)
			return
		}
	}
	// 没找到，直接全量
	DPrintf("没找到, [%d] 要求leader给全量", rf.me)
	reply.EarlierTerm = -1
	reply.EarlierIndex = -1
	return
}

func (rf *Raft) FollowerApply() {

	timer := time.NewTimer(ApplyDuration)
	//defer timer.Stop()

	for !rf.isKilled {

		<-timer.C

		if rf.isKilled {
			return
		}

		//_, isLeader := rf.GetState()

		//if !isLeader {

		rf.mu.Lock()
		rf.apply()
		//rf.mu.Unlock()
		////}
		//
		//rf.mu.Lock()
		timer.Reset(HeartBeatDuration)
		rf.mu.Unlock()
	}

}

func (rf *Raft) LogLoop() {
	for {
		DPrintf("%d state:%d 的 logEntries: %+v, commitIndex = %d, lastApplied = %d",
			rf.me, rf.state, rf.log, rf.commitIndex, rf.lastApplied)
		time.Sleep(time.Millisecond * 500)
	}
}
