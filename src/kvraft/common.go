package raftkv

const (
    OK       = "OK"
    ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    Key string
    Value string
    Op string
    Name int64
    Seq int
}

type PutAppendReply struct {
    WrongLeader bool
    Err Err
}

type GetArgs struct {
    Key string
    Name int64
    Seq int
}

type GetReply struct {
    WrongLeader bool
    Err         Err
    Value       string
}
