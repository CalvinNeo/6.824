package raftkv

import "encoding/gob"
import "bytes"
import "labrpc"
import "log"
import "raft"
import "sync"
import "time"
import "fmt"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
        fmt.Printf(format, a...)
    }
    return
}

type Op struct {
    Method string
    Key string
    Value string
    // Identify clients
    Name int64
    // Sequence number for every clients
    Seq int
}

type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    maxraftstate int // snapshot if log grows this big

    db map[string]string
    ack map[int64]int
    result map[int]chan Op
}


func (kv *KVServer) IsValid(name int64, seq int) bool {
    client, ok := kv.ack[name]
    if ok {
        // 如果seq小于client的ack，那么肯定是已经收到的
        return client < seq
    }
    // 没有这个client
    return true
}


func (kv *KVServer) AppendEntryToLog(entry Op) bool {
    index, _, isLeader := kv.rf.Start(entry)
    if !isLeader {
        return false
    }

    // 通过kv.result等待日志添加结果
    kv.mu.Lock()
    ch, ok := kv.result[index]
    if !ok{
        // 注意下面还需要用到ch
        ch = make(chan Op, 1)
        kv.result[index] = ch
    }
    kv.mu.Unlock()
    select {
    case op := <-ch:
        return op == entry
    case <- time.After(1000 * time.Millisecond):
        // 超时
        return false
    }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // 注意Get也要入日志以保证线性一致
    entry := Op{Method:"Get", Key:args.Key, Name:args.Name, Seq:args.Seq}

    ok := kv.AppendEntryToLog(entry)
    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
        kv.mu.Lock()
        reply.Value = kv.db[args.Key]
        kv.mu.Unlock()
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    entry := Op{Method:args.Op, Key:args.Key, Value:args.Value, Name:args.Name, Seq:args.Seq}

    ok := kv.AppendEntryToLog(entry)
    if !ok {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (kv *KVServer) Kill() {
    kv.rf.Kill()
    // Your code here, if desired.
}

func (kv *KVServer) Apply(args Op) {
    switch args.Method {
    case "Put":
        kv.db[args.Key] = args.Value
    case "Append":
        kv.db[args.Key] += args.Value
    }
    // 更新Seq
    kv.ack[args.Name] = args.Seq
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    gob.Register(Op{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate

    // You may need initialization code here.
    kv.db = make(map[string]string)
    kv.ack = make(map[int64]int)
    kv.result = make(map[int]chan Op)
    kv.applyCh = make(chan raft.ApplyMsg, 100)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go func() {
        for {
            // 当收到Raft的Apply通知，msg表示`msg.CommandIndex`这个Log已经被Apply
            msg := <-kv.applyCh
            if msg.UseSnapshot {
                var last_include_index int
                var last_include_term int
                data := gob.NewDecoder(bytes.NewBuffer(msg.Snapshot))
                kv.mu.Lock()
                kv.db = make(map[string]string)
                kv.ack = make(map[int64]int)
                data.Decode(&last_include_index)
                data.Decode(&last_include_term)
                data.Decode(&kv.db)
                data.Decode(&kv.ack)
                kv.mu.Unlock()
            } else {
                op := msg.Command.(Op)
                kv.mu.Lock()
                if kv.IsValid(op.Name, op.Seq) {
                    kv.Apply(op)
                }
                // 通过result通知 AppendEntryToLog
                ch, ok := kv.result[msg.CommandIndex]
                if !ok {
                    kv.result[msg.CommandIndex] = make(chan Op, 1)
                }else{
                    select {
                        case <-kv.result[msg.CommandIndex]:
                        default:
                    }
                    ch <- op
                }
                // fmt.Printf("maxraftstate %v, kv.rf.GetPerisistSize() %v\n", maxraftstate, kv.rf.GetPerisistSize())
                if maxraftstate != -1 && kv.rf.GetPerisistSize() > maxraftstate {
                    w := new(bytes.Buffer)
                    e := gob.NewEncoder(w)
                    // 注意在StartSnapshot中会在前面填充(last_include_index, last_include_term)这两个字段
                    e.Encode(kv.db)
                    e.Encode(kv.ack)
                    data := w.Bytes()
                    go kv.rf.StartSnapshot(data, msg.CommandIndex)
                }
                kv.mu.Unlock()
            }
        }
    }()

    return kv
}
