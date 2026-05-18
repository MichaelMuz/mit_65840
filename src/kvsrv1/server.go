package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	log.SetFlags(log.Lshortfile)

	kv := MakeKVServer()
	return []any{kv}
}

func MakeKVServer() *KVServer {
	// no need to worry ab this being stack alloc with dangling pointer bc go knows unlike C
	kv := &KVServer{kv: make(map[string]value)}
	return kv
}

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type value struct {
	val     string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	kv map[string]value
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	DPrintf("Server get got: %#v %#v", args, reply)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = e.val
		reply.Version = e.version
		reply.Err = rpc.OK
	}
	DPrintf("Server get returning: %#v", reply)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	DPrintf("Server put %#v %#v", args, reply)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.kv[args.Key]
	if ok {
		if args.Version == e.version {
			kv.kv[args.Key] = value{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.kv[args.Key] = value{args.Value, 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

	DPrintf("Server put returning %#v", args)
}
