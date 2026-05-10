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
	Mapper bool   // true if map, false if reduce
	File   string // file name if mapping hash if reducing
	Task   int    // task number
}

type SignalFileReadyArgs struct {
	Mapper bool   // true if map, false if reduce
	Orig   string // original file name if mapping hash if reducing
	Task   int    // task number
	Oname  string // output file name
}

type SignalFileReadyReply struct{}
