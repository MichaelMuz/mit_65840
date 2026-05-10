package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

// plan:
// There will be a work queue formed by a channel
// The Request work function will read the next thing from the channel
// It will need to push it into a monitoring work channel where a persistent go routine is gonna check
//    if any machines have fallen too behind then push back into the work queue channel
//    Anything that truly finished will have that task pushed into a finished channel
// Once all the mapping tasks are signaled to be done (the finished channel is the same of num files)
//    we will push all the reduce tasks into the todo channel to be handed out
//
// This is similar to what I did initially with the slices but with channels

type TsWork struct {
	task WorkRequestReply
	ts   time.Time
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	autoinc atomic.Int32

	tasks        chan WorkRequestReply
	pending      chan TsWork
	completedIds chan int
	finished     chan WorkRequestReply

	done bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestWork(args *WorkRequestArgs, reply *WorkRequestReply) error {
	var t WorkRequestReply
	select {
	case t = <-c.tasks:
	default:
		reply.Ready = false
		return nil
	}

	// work is preformed. I just need to add a new uuid to it
	t.Uuid = int(c.autoinc.Add(1) - 1)
	*reply = t

	c.pending <- TsWork{t, time.Now()}

	return nil
}

func (c *Coordinator) SignalFinished(arg *SignalFileReadyArgs, reply *SignalFileReadyReply) error {
	c.completedIds <- arg.uuid
	// _, in := c.filesProgressingMap[arg.Orig]
	// if !in {
	// 	fmt.Printf("laggard finished %v too late", arg.Orig)
	// 	return nil
	// }
	// delete(c.filesProgressingMap, arg.Orig)
	// c.filesWaitingReduce = append(c.filesWaitingReduce, arg.Orig)
	// fmt.Printf("file %v processed by task %v, intermediate", arg.Orig, arg.Task)

	return nil
}

// keeps track of pending tasks and pushes them back on work queue if not done fast enough
func (c *Coordinator) controller() {
	pending := map[int]TsWork{}
	for {
		select {
		case p := <-c.pending:
			pending[p.task.Uuid] = p
		case co := <-c.completedIds:
			tsk, found := pending[co]
			if found {
				delete(pending, co)
				c.finished <- tsk.task
			}
		default:
			t := time.Now()
			for k, v := range pending {
				if t.Sub(v.ts) > time.Second*10 {
					delete(pending, k)
					c.tasks <- v.task
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce, atomic.Int32{}, make(chan WorkRequestReply, len(files)), make(chan TsWork, len(files)), make(chan int, len(files)), make(chan WorkRequestReply, len(files)), false}

	// Your code here.

	// make the packaged up mapper tasks
	mTasks := []WorkRequestReply{}
	n := 0
	for _, f := range files {
		t := WorkRequestReply{true, true, f, n, nReduce, -1}
		mTasks = append(mTasks, t)
		n++
	}

	// make the packaged up reducer tasks
	rTasks := []WorkRequestReply{}
	for r := range nReduce {
		t := WorkRequestReply{true, false, "", r, nReduce, -1}
		rTasks = append(rTasks, t)
	}

	c.server(sockname)
	go c.controller()

	for _, t := range mTasks {
		c.tasks <- t
	}

	for range len(files) {
		_ = <-c.finished
	}

	for _, t := range rTasks {
		c.tasks <- t
	}

	for range nReduce {
		_ = <-c.finished
	}

	return &c
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

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
