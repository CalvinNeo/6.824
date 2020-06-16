package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
    Name int64
    Seq int
    mu sync.Mutex
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // You'll have to add code here.
    ck.Name = nrand()
    ck.Seq = 0
    return ck
}

//
func (ck *Clerk) Get(key string) string {
    var args GetArgs
    args.Key = key
    args.Name = ck.Name
    ck.mu.Lock()
    args.Seq = ck.Seq
    ck.Seq ++
    ck.mu.Unlock()
    for {
        for _, v := range ck.servers {
            var reply GetReply
            ok := v.Call("KVServer.Get", &args, &reply)
            if ok && reply.WrongLeader == false {
                return reply.Value
            }
        }
    }
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    var args PutAppendArgs
    args.Key = key
    args.Name = ck.Name
    args.Value = value
    args.Op = op
    ck.mu.Lock()
    args.Seq = ck.Seq
    ck.Seq ++
    ck.mu.Unlock()
    for {
        for _, v := range ck.servers {
            var reply PutAppendReply
            ok := v.Call("KVServer.PutAppend", &args, &reply)
            if ok && reply.WrongLeader == false {
                return
            }
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}
