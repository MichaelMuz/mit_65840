package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMap(mapf func(string, string) []KeyValue, filename string, task int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	rhash := ihash(filename)

	oname := fmt.Sprintf("mr-%v-%v", task, rhash)

	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("%v", err)
	}

	enc := json.NewEncoder(ofile)
	err = enc.Encode(&kva)
	if err != nil {
		log.Fatalf("%v", err)
	}

	err = ofile.Close()
	if err != nil {
		log.Fatalf("%v", err)
	}

	signalDone(true, filename, task, oname)
}

func handleReduce(reducef func(string, []string) string, hash string, task int) {
	// collect all the files that I need
	dir, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("%v", err)
	}
	// kvs := []KeyValue{}
	kvs := map[string][]string{}
	for _, de := range dir {
		if de.IsDir() {
			continue
		}
		if spl := strings.Split(de.Name(), "-"); spl[len(spl)-1] != hash {
			continue
		}
		bytes, err := os.ReadFile(de.Name())

		kv := KeyValue{}
		err = json.Unmarshal(bytes, &kv)
		if err != nil {
			log.Fatalf("%v", err)
		}

		slc, exists := kvs[kv.Key]
		if !exists {
			slc = []string{}
			kvs[kv.Key] = slc
		}
		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
	}

}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// uncomment to send the Example RPC to the coordinator.
	CallExample()

	// Your worker implementation here.

	// We are given the map/reduce function here, we don't know if we are a map or reduce worker yet

	// Worker needs to:
	// 1. Reach out to coordinator and ask for tasks, coordinator will give it a file name and whether to map or reduce
	// 2. map on that input
	// 3. At the end sort the outputs into intermediate files. name mr-X-Y where X is map task number and Y is reduce task number
	mapper, filename, task := GetWork()
	if mapper {
		handleMap(mapf, filename, task)
	} else {
		handleReduce(reducef, filename, task)
	}

}

func GetWork() (bool, string, int) {
	args := WorkRequestArgs{}
	reply := WorkRequestReply{}
	for {
		ok := call("Coordinator.RequestWork", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		}

		if len(reply.File) == 0 {
			time.Sleep(1 * time.Second)
		} else {
			fmt.Printf("Task %v assigned to work on %v\n", reply.Task, reply.File)
			break
		}
	}
	return reply.Mapper, reply.File, reply.Task
}

func signalDone(mapper bool, orig string, task int, oname string) {
	args := SignalFileReadyArgs{mapper, orig, task, oname}
	reply := SignalFileReadyReply{}
	ok := call("Coordinator.SignalFinished", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
	fmt.Printf("Task %v signaling %v was processed into %v \n", task, orig, oname)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
