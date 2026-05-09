package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	autoinc      int
	filesWaiting []string
	filesPending []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestWork(args *WorkRequestArgs, reply *WorkRequestReply) error {
	if len(c.filesWaiting) == 0 {
		reply.File = ""
		return nil
	}

	assigned := c.filesWaiting[len(c.filesWaiting)-1]
	c.filesWaiting = c.filesWaiting[:len(c.filesWaiting)-1]
	c.filesPending = append(c.filesPending, assigned)

	reply.File = assigned
	reply.Task = c.autoinc

	c.autoinc += 1
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
	c := Coordinator{}

	// Your code here.
	c.filesWaiting = files

	c.server(sockname)
	return &c
}
