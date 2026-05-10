package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int

	autoinc                int
	filesWaitingMap        []string
	filesProgressingMap    map[string]int
	filesWaitingReduce     []string
	filesProgressingReduce map[string]int
	filesFinished          []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SignalFinished(arg *SignalFileReadyArgs, reply *SignalFileReadyReply) error {
	_, in := c.filesProgressingMap[arg.Orig]
	if !in {
		fmt.Printf("laggard finished %v too late", arg.Orig)
		return nil
	}
	delete(c.filesProgressingMap, arg.Orig)
	c.filesWaitingReduce = append(c.filesWaitingReduce, arg.Orig)
	fmt.Printf("file %v processed by task %v, intermediate result in file %v", arg.Orig, arg.Task, arg.Oname)

	return nil
}

func (c *Coordinator) RequestWork(args *WorkRequestArgs, reply *WorkRequestReply) error {
	if len(c.filesWaitingMap) == 0 {
		reply.File = ""
		return nil
	}

	assigned := c.filesWaitingMap[len(c.filesWaitingMap)-1]
	reply.File = assigned

	reply.Task = c.autoinc
	c.autoinc += 1

	c.filesWaitingMap = c.filesWaitingMap[:len(c.filesWaitingMap)-1]
	c.filesProgressingMap[assigned] = reply.Task

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, filesProgressingMap: make(map[string]int), filesProgressingReduce: make(map[string]int)}

	// Your code here.
	c.filesWaitingMap = files

	c.server(sockname)
	return &c
}
