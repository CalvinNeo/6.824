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

import "sync"
import "labrpc"
import "time"
import "fmt"
import "math/rand"

import "bytes"
import "encoding/gob"

const (
    Leader = iota
    Candidate
    Follower

    HBINTERVAL = 50 * time.Millisecond // 50ms
    default_index = 0
    default_term = 0
)

type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
    UseSnapshot bool
    Snapshot    []byte
}

type LogEntry struct {
    Index int
    Term int
    Command interface{}
}


type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state int
    vote_got int
    vote_for int
    current_term int
    logs [] LogEntry
    commit_index int
    last_applied int
    next_index []int
    match_index []int

    chanCommit chan bool
    chanHeartbeat chan bool
    chanGrantVote chan bool
    chanLeader chan bool
    chanApply chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
    return rf.current_term, rf.state == Leader
}
func (rf *Raft) GetState2() (int) {
    return rf.state
}

func (rf *Raft) encode_raft_state() []byte{
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.current_term)
    e.Encode(rf.vote_for)
    e.Encode(rf.logs)
    data := w.Bytes()
    return data
}
func (rf *Raft) load_raft_state(data []byte) {
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.current_term)
    d.Decode(&rf.vote_for)
    d.Decode(&rf.logs)
}
func (rf *Raft) load_snapshot(data []byte) {
    // 重要
    if len(data) == 0 {
        return
    }
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)

    var LastIncludedIndex int
    var LastIncludedTerm int

    d.Decode(&LastIncludedIndex)
    d.Decode(&LastIncludedTerm)
    // if rf.commit_index < LastIncludedIndex{
        rf.commit_index = LastIncludedIndex
    // }
    // if rf.last_applied < LastIncludedIndex{
        rf.last_applied = LastIncludedIndex
    // }
    
    fmt.Printf("Load Snapshot LastIncludedIndex %v LastIncludedTerm %v rf.commit_index %v rf.last_applied %v base %v last index %v last term %v\n", 
        LastIncludedIndex, LastIncludedTerm, rf.commit_index, rf.last_applied, rf.get_base_index(), rf.last_log_index(), rf.last_log_term())
    rf.logs = truncate_log(LastIncludedIndex, LastIncludedTerm, rf.logs)
    fmt.Printf("Load Snapshot logs %v\n", rf.logs)
    msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

    go func() {
        rf.chanApply <- msg
    }()
}
func (rf *Raft) persist() {
    // fmt.Printf("Persist log size %v\n", len(rf.logs))
    rf.persister.SaveRaftState(rf.encode_raft_state())
}

type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Name int
    Term int
    Last_log_index int
    Last_log_term int
}

type RequestVoteReply struct {
    // Your data here (2A).
    Name int
    Term int
    Vote_granted bool
}

type AppendEntriesArgs struct {
    // Your data here.
    Term int
    Name int
    Prev_log_term int
    Prev_log_index int
    Entries []LogEntry
    Leader_commit int
}

type AppendEntriesReply struct {
    // Your data here.
    Name int
    Term int
    Success bool
    Last_log_index int
    Last_log_term int
}

type InstallSnapshotArgs struct {
    Name int
    Term int
    Last_included_index int
    Last_included_term  int
    Data []byte
}

type InstallSnapshotReply struct {
    Name int
    Term int
}

func (rf *Raft) become_follower(term int){
    rf.state = Follower
    rf.vote_for = -1
    rf.current_term = term
}

func (rf *Raft) become_leader(){
    rf.state = Leader
    rf.next_index = make([]int, len(rf.peers))
    rf.match_index = make([]int, len(rf.peers))
    for i := range rf.peers {
        rf.next_index[i] = rf.last_log_index() + 1
        rf.match_index[i] = default_index
    }
}

func (rf *Raft) last_log_index() int {
    if len(rf.logs) == 0{
        return -1
    }else{
        return rf.logs[len(rf.logs) - 1].Index
    }
}

func (rf *Raft) last_log_term() int {
    if len(rf.logs) == 0{
        return 0
    }else{
        return rf.logs[len(rf.logs) - 1].Term
    }
}

func (rf *Raft) get_base_index() int{
    return rf.logs[0].Index
}
func (rf *Raft) gl(index int) *LogEntry{
    i := index - rf.get_base_index()
    if i < 0 || i >= len(rf.logs){
        fmt.Printf("gl Error i %v len %v\n", i, len(rf.logs))
    }
    return &(rf.logs[i])
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()
    // defer rf.persister.SaveRaftState(rf.encode_raft_state())
    reply.Vote_granted = false
    reply.Name = rf.me

    if args.Term < rf.current_term {
        reply.Term = rf.current_term
        return
    }
    if args.Term > rf.current_term {
        rf.become_follower(args.Term)
    }
    reply.Term = rf.current_term
    if rf.vote_for != -1 && rf.vote_for != args.Name {
        return
    }
    if args.Last_log_term < rf.last_log_term() {
        return
    }
    if args.Last_log_term == rf.last_log_term() && args.Last_log_index < rf.last_log_index(){
        return
    }
    // TODO Changed
    rf.chanGrantVote <- true
    rf.vote_for = args.Name
    reply.Vote_granted = true
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if ok {
        if rf.state != Candidate {
            return ok
        }
        // TODO Changed
        if args.Term != rf.current_term {
            return ok
        }
        if reply.Term > rf.current_term{
            rf.become_follower(reply.Term)
            rf.persist()
            // rf.persister.SaveRaftState(rf.encode_raft_state())
        }
        if reply.Vote_granted {
            rf.vote_got ++
            if rf.state == Candidate && rf.vote_got > len(rf.peers) / 2{
                rf.state = Follower
                rf.chanLeader <- true
            }
        }
    }
    return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    // Your code here.
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()
    // defer rf.persister.SaveRaftState(rf.encode_raft_state())

    reply.Name = rf.me
    reply.Success = false


    if args.Term < rf.current_term{
        reply.Term = rf.current_term
        reply.Last_log_index = rf.last_log_index()
        reply.Last_log_term = rf.last_log_term()
        return
    }
    rf.chanHeartbeat <- true
    if args.Term > rf.current_term {
        rf.become_follower(args.Term)
    }

    reply.Term = rf.current_term
    if args.Prev_log_index >= 0 && args.Prev_log_index > rf.last_log_index() {
        reply.Last_log_index = rf.last_log_index()
        reply.Last_log_term = rf.last_log_term()
        return
    }

    if args.Prev_log_index > rf.get_base_index() {
        wrong_term := rf.gl(args.Prev_log_index).Term
        if args.Prev_log_term != wrong_term {
            for i := args.Prev_log_index - 1; i >= rf.get_base_index(); i-- {
                if rf.gl(i).Term != wrong_term {
                    // 似乎应当立即移除，见Nuft实现的对应部分
                    rf.logs = rf.logs[: i - rf.get_base_index() + 1]
                    reply.Last_log_index = i
                    break
                }
            }
            return
        }
    }

    if args.Prev_log_index >= rf.get_base_index() {
        rf.logs = rf.logs[: args.Prev_log_index - rf.get_base_index() + 1]
        rf.logs = append(rf.logs, args.Entries...)
        reply.Success = true
        reply.Last_log_index = rf.last_log_index()
    }

    if args.Leader_commit > rf.commit_index{
        // fmt.Printf("Receive Leader commit %v Advance from %v size %v\n", args.Leader_commit, rf.commit_index, len(rf.logs))
        rf.commit_index = args.Leader_commit
        if rf.commit_index > rf.last_log_index(){
            rf.commit_index = rf.last_log_index()
        }
        rf.chanCommit <- true
    }
    reply.Success = true
    reply.Last_log_index = rf.last_log_index()
    reply.Last_log_term = rf.last_log_term()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if ok {
        if rf.state != Leader {
            return ok
        }

        if reply.Term > rf.current_term {
            rf.become_follower(reply.Term)
            rf.vote_for = -1
            rf.persist()
            // rf.persister.SaveRaftState(rf.encode_raft_state())
            return ok
        } else if(reply.Term != rf.current_term){
            return ok
        }
        if (!reply.Success){
            rf.next_index[server] = reply.Last_log_index + 1
            rf.match_index[server] = reply.Last_log_index
            return ok
        }
        if(reply.Last_log_index > rf.last_log_index()){
            rf.next_index[server] = rf.last_log_index() + 1
            rf.match_index[server] = rf.last_log_index()
            return ok
        }else{
            rf.next_index[server] = reply.Last_log_index + 1
            rf.match_index[server] = reply.Last_log_index
        }

        new_commit := reply.Last_log_index
        if(rf.commit_index >= new_commit){
            // fmt.Printf("Name %v, rf.commit_index %v >= new_commit %v\n", rf.me, rf.commit_index, new_commit)
            return ok
        }
        if(rf.current_term != rf.gl(reply.Last_log_index).Term){
            // fmt.Printf("rf.current_term %v != rf.logs[reply.Last_log_index].Term %v \n", rf.current_term, rf.logs[reply.Last_log_index].Term)
            return ok
        }

        commit_vote := 1
        for j := range rf.peers{
            if j != rf.me && rf.match_index[j] >= new_commit{
                commit_vote++
            }
        }
        if commit_vote > len(rf.peers) / 2{
            fmt.Printf("Leader Commit to %v\n", new_commit)
            rf.commit_index = new_commit
            rf.chanCommit <- true
        }
    }
    return ok
}

func (rf *Raft) broadcastAppendEntries() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    for j := range rf.peers{
        if j == rf.me{
            continue
        }
        // 这里必须是<=，否则`prev_log_term = rf.gl(prev_log_index).Term`语句会越界
        if rf.next_index[j] <= rf.get_base_index() {
            var args InstallSnapshotArgs
            args.Name = rf.me
            args.Term = rf.current_term
            args.Last_included_index = rf.get_base_index()
            args.Last_included_term = rf.gl(rf.get_base_index()).Term
            args.Data = rf.persister.snapshot
            go func(server int,args InstallSnapshotArgs) {
                reply := &InstallSnapshotReply{}
                rf.sendInstallSnapshot(server, args, reply)
            }(j, args)
        }else{
            var args AppendEntriesArgs
            prev_log_term := 0
            prev_log_index := rf.next_index[j] - 1
            if prev_log_index >= 0{
                prev_log_term = rf.gl(prev_log_index).Term
            }
            args.Name = rf.me
            args.Term = rf.current_term
            args.Prev_log_index = prev_log_index
            args.Prev_log_term = prev_log_term
            args.Leader_commit = rf.commit_index

            // TODO make sure rf.next_index[j] >= 0 
            args.Entries = make([]LogEntry, len(rf.logs[rf.next_index[j] - rf.get_base_index():]))
            copy(args.Entries, rf.logs[rf.next_index[j] - rf.get_base_index():])
            go func(j int,args AppendEntriesArgs) {
                var reply AppendEntriesReply
                rf.sendAppendEntries(j, args, &reply)
            }(j, args)
        }
    }
}

func (rf *Raft) broadcastRequestVote() {
    var args RequestVoteArgs
    rf.mu.Lock()
    args.Term = rf.current_term
    args.Name = rf.me
    args.Last_log_term = rf.last_log_term()
    args.Last_log_index = rf.last_log_index()
    rf.mu.Unlock()

    for i := range rf.peers {
        if i != rf.me && rf.state == Candidate {
            go func(i int) {
                var reply RequestVoteReply
                // fmt.Printf("%v RequestVote to %v\n",rf.me,i)
                rf.sendRequestVote(i, args, &reply)
            }(i)
        }
    }
}

func find_entry(logs []LogEntry, index int, term int) int{
    for i := len(logs) - 1; i >= 0; i-- {
        if logs[i].Term == term && logs[i].Index == index {
            return i
        }
    }
    return -1
}
func truncate_log(last_included_index int, last_included_term int, logs []LogEntry) []LogEntry {

    var new_entries []LogEntry
    // 返回从(last_included_index, last_included_term)开始的到末尾的所有log
    // 第一个用snapshot顶替
    new_entries = append(new_entries, LogEntry{Index: last_included_index, Term: last_included_term})
    i := find_entry(logs, last_included_index, last_included_term)
    if i != -1 && i + 1 < len(logs) {
        new_entries = append(new_entries, logs[i+1:]...)
    }
    fmt.Printf("truncate until (index %v, term %v), i %v, len %v, new len %v\n", 
        last_included_index, last_included_term, i, len(logs), len(new_entries))
    return new_entries
}
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
    // Leader通知我Install它的Snapshot
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if args.Term < rf.current_term {
        reply.Term = rf.current_term
        return
    }
    fmt.Printf("Leader: Snapshot from base %v to %v\n", rf.get_base_index(), args.Last_included_index)
    rf.chanHeartbeat <- true
    rf.state = Follower
    // TODO 这个有必要么?
    // rf.current_term = args.Term

    rf.logs = truncate_log(args.Last_included_index, args.Last_included_term, rf.logs)

    msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

    // if rf.last_applied < args.Last_included_index{
        rf.last_applied = args.Last_included_index
    // }
    // if rf.commit_index < args.Last_included_index{
        rf.commit_index = args.Last_included_index
    // }

    // log变了，所以需要重新persist
    rf.persister.SaveStateAndSnapshot(rf.encode_raft_state(), args.Data)

    rf.chanApply <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    if ok {
        if reply.Term > rf.current_term {
            rf.become_follower(reply.Term)
            return ok
        }

        rf.next_index[server] = args.Last_included_index + 1
        rf.match_index[server] = args.Last_included_index
    }
    return ok
}
func (rf *Raft) GetPerisistSize() int {
    return rf.persister.RaftStateSize()
}
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
    // 以index为last_included_index
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if index <= rf.get_base_index() || index > rf.last_log_index() || index > rf.last_applied {
        // index不能落在已经Snapshot的区域，可能Leader通过sendInstallSnapshot添加的Snapshot
        // index必须是applied的
        return
    }

    fmt.Printf("Snapshot from base %v to %v\n", rf.get_base_index(), index)
    var new_entries []LogEntry
    // 我们把snapshot中的state machine state直接dump到文件中
    new_entries = append(new_entries, LogEntry{Index: index, Term: rf.gl(index).Term})
    for i := index + 1; i <= rf.last_log_index(); i++ {
        new_entries = append(new_entries, *rf.gl(i))
    }
    rf.logs = new_entries
    last_included_index := new_entries[0].Index
    last_included_term := new_entries[0].Term

    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(last_included_index)
    e.Encode(last_included_term)
    data := w.Bytes()
    data = append(data, snapshot...)
    rf.persister.SaveStateAndSnapshot(rf.encode_raft_state(), data)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    index := -1
    if rf.state == Leader {
        index = rf.last_log_index()+1
        // fmt.Printf("%v start log %v\n", rf.me, index)
        rf.logs = append(rf.logs, LogEntry{Term:rf.current_term,Command:command,Index:index}) 
        rf.persist()
        // rf.persister.SaveRaftState(rf.encode_raft_state())
    }
    // fmt.Printf("Start %v Logs %v\n", command, rf.logs)
    return index, rf.current_term, rf.state == Leader
}

func (rf *Raft) Kill() {
    // Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    fmt.Printf("A")
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.state = Follower
    rf.vote_for = -1
    rf.logs = append(rf.logs, LogEntry{Term: 0})
    rf.current_term = 0
    rf.commit_index = default_index
    rf.vote_got = 0

    rf.chanCommit = make(chan bool, 100)
    rf.chanHeartbeat = make(chan bool, 100)
    rf.chanGrantVote = make(chan bool, 100)
    rf.chanLeader = make(chan bool, 100)
    rf.chanApply = applyCh

    // initialize from state persisted before a crash
    rf.load_raft_state(persister.ReadRaftState())
    rf.load_snapshot(persister.ReadSnapshot())


    go func() {
        for {
            switch rf.state {
            case Follower:
                select {
                case <-rf.chanHeartbeat:
                case <-rf.chanGrantVote:
                case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                    rf.state = Candidate
                }
            case Leader:
                rf.broadcastAppendEntries()
                time.Sleep(HBINTERVAL)
            case Candidate:
                rf.mu.Lock()
                rf.current_term++
                rf.vote_for = rf.me
                rf.vote_got = 1
                rf.persist()
                // rf.persister.SaveRaftState(rf.encode_raft_state())
                rf.mu.Unlock()
                go rf.broadcastRequestVote()
                    // fmt.Printf("%v become CANDIDATE %v\n",rf.me,rf.current_term)
                    select {
                    case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                    case <-rf.chanHeartbeat:
                        rf.state = Follower
                    case <-rf.chanLeader:
                        rf.mu.Lock()
                        rf.become_leader()
                        rf.mu.Unlock()
                    }
                
            }
        }
    }()
    go func() {
        for {
            select {
            case <-rf.chanCommit:
                // 注意必须Apply之后才能通过nCommitted函数
                rf.mu.Lock()
                for i := rf.last_applied + 1; i <= rf.commit_index; i++ {
                    fmt.Printf("Apply base %v commit_index %v last index %v log len %v i %v cmd %v\n", 
                        rf.get_base_index(), rf.commit_index, rf.last_log_index(), len(rf.logs), i, rf.gl(i).Command)
                    msg := ApplyMsg{CommandIndex: i, Command: rf.gl(i).Command, CommandValid: true}
                    applyCh <- msg
                    rf.last_applied = i
                }
                rf.mu.Unlock()
            }
        }
    }()
    return rf
    return rf
}
