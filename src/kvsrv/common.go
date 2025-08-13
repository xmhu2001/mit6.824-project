package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
