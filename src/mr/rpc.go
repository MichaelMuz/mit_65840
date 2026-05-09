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
	mapper bool

	File string
	Task int
}

type SignalFileReadyArgs struct {
	Orig  string
	Task  int
	Oname string
}

type SignalFileReadyReply struct{}
