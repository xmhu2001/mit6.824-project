package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu             sync.Mutex
	kv             map[string]string
	lastRequestID  map[int64]int64
	lastRequestRes map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kv[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 	去重
	if lastID, ok := kv.lastRequestID[args.ClientID]; ok && args.RequestID <= lastID {
		return
	}

	kv.kv[args.Key] = args.Value
	kv.lastRequestID[args.ClientID] = args.RequestID
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 去重
	if lastID, ok := kv.lastRequestID[args.ClientID]; ok && args.RequestID <= lastID {
		reply.Value = kv.lastRequestRes[args.ClientID]
		return
	}

	oldValue, ok := kv.kv[args.Key]
	if ok {
		reply.Value = oldValue
	} else {
		reply.Value = ""
	}
	kv.kv[args.Key] = oldValue + args.Value
	kv.lastRequestID[args.ClientID] = args.RequestID
	// append 语义
	// 用于去重时返回旧处理结果
	kv.lastRequestRes[args.ClientID] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv = make(map[string]string)
	kv.lastRequestID = make(map[int64]int64)
	kv.lastRequestRes = make(map[int64]string)

	return kv
}
