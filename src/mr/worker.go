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

func handleMap(mapf func(string, string) []KeyValue, filename string, task int, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))

	agg := map[int][]KeyValue{}
	for _, kv := range kva {
		h := ihash(kv.Key) % nReduce

		slc, exists := agg[h]
		if !exists {
			slc = []KeyValue{}
			agg[h] = slc
		}
		agg[h] = append(agg[h], kv)
	}

	for h, slc := range agg {
		oname := fmt.Sprintf("mr-%v-%v", task, h)
		tmpName := fmt.Sprintf("%v-tmp", oname)

		ofile, err := os.Create(tmpName)
		if err != nil {
			log.Fatalf("%v", err)
		}

		enc := json.NewEncoder(ofile)
		err = enc.Encode(&slc)
		if err != nil {
			log.Fatalf("%v", err)
		}
		os.Rename(tmpName, oname)
	}

	signalDone(true, filename, task)
}

func handleReduce(reducef func(string, []string) string, reducerNum int) {
	// collect all the files that I need which must match my reducer num in name
	rNumSt := fmt.Sprintf("%v", reducerNum)

	dir, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("%v", err)
	}

	kvs := map[string][]string{}
	for _, de := range dir {
		if de.IsDir() {
			continue
		}
		if spl := strings.Split(de.Name(), "-"); spl[len(spl)-1] != rNumSt {
			continue
		}
		bytes, err := os.ReadFile(de.Name())

		slc := []KeyValue{}
		err = json.Unmarshal(bytes, &slc)
		if err != nil {
			log.Fatalf("%v", err)
		}

		for _, pair := range slc {
			vals, exists := kvs[pair.Key]
			if !exists {
				vals = []string{}
				kvs[pair.Key] = vals
			}
			kvs[pair.Key] = append(kvs[pair.Key], pair.Value)
		}
	}

	reds := []string{}
	for k, vals := range kvs {
		output := reducef(k, vals)
		pretty := fmt.Sprintf("%v %v\n", k, output) // exact expected output
		reds = append(reds, pretty)
	}

	oname := fmt.Sprintf("mr-out-%v", reducerNum)
	onameTmp := fmt.Sprintf("%v-tmp", oname)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("%v", err)
	}
	_, err = fmt.Fprint(ofile, strings.Join(reds, ""))
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := ofile.Close(); err != nil {
		log.Fatalf("%v", err)
	}

	os.Rename(onameTmp, oname)
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
	nReduce, mapper, filename, task, reducerNum := GetWork()
	if mapper {
		handleMap(mapf, filename, task, nReduce)
	} else {
		handleReduce(reducef, reducerNum)
	}

}

func GetWork() (int, bool, string, int, int) {
	args := WorkRequestArgs{}
	reply := WorkRequestReply{}
	for {
		ok := call("Coordinator.RequestWork", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		}

		if !reply.Ready {
			time.Sleep(1 * time.Second)
		} else {
			fmt.Printf("Task %v assigned to work on %v\n", reply.Task, reply.File)
			break
		}
	}
	return reply.TotalReducers, reply.Mapper, reply.File, reply.Task, reply.reducerNum
}

func signalDone(mapper bool, orig string, task int) {
	args := SignalFileReadyArgs{mapper, orig, task}
	reply := SignalFileReadyReply{}
	ok := call("Coordinator.SignalFinished", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
	fmt.Printf("Task %v signaling %v was processed \n", task, orig)
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
