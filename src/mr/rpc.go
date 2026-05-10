package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkRequestArgs struct{}
type WorkRequestReply struct {
	Ready         bool // if we have any work
	Mapper        bool   // true if map, false if reduce
	File          string // file name if mapping, ignore in reduce case
	Task          int    // task number
	TotalReducers int    // number of reducers, important for hashing
	reducerNum    int    // which reducer I am
}

type SignalFileReadyArgs struct {
	Mapper bool   // true if map, false if reduce
	Orig   string // original file name if mapping hash if reducing
	Task   int    // task number
}

type SignalFileReadyReply struct{}
