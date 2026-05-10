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
	Ready         bool   // if we have any work
	Mapper        bool   // true if map, false if reduce
	File          string // file name if mapping, ignore in reduce case
	Task          int    // task number for mappers or reducer num for reducers
	TotalReducers int    // number of reducers, important for hashing
	Uuid          int    // unique num assigned to each worker invocation
}

type SignalFileReadyArgs struct {
	Uuid int
}
type SignalFileReadyReply struct{}
