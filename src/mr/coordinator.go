package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

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
	c.completedIds <- arg.Uuid
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

func (c *Coordinator) run(files []string, nReduce int) {
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

	go c.controller()

	fmt.Println("Starting map tasks")
	for _, t := range mTasks {
		c.tasks <- t
	}

	fmt.Println("Waiting for map tasks to finish")
	for range len(files) {
		_ = <-c.finished
	}

	fmt.Println("Starting reduce tasks")
	for _, t := range rTasks {
		c.tasks <- t
	}

	fmt.Println("Waiting for reduce tasks to finish")
	for range nReduce {
		_ = <-c.finished
	}

	fmt.Println("All tasks finished")
	c.done = true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce, atomic.Int32{}, make(chan WorkRequestReply, len(files)), make(chan TsWork, len(files)), make(chan int, len(files)), make(chan WorkRequestReply, len(files)), false}
	c.server(sockname)
	// Your code here.
	go c.run(files, nReduce)

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
