package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		res := call("Coordinator.GetTask", &args, &reply)
		if res == false {
			log.Fatalf("GetTask RPC call failed")
			break
		}
		if reply.allDone {
			fmt.Println("All tasks are done. Worker exiting.")
			break
		}
		switch reply.Task.Type {
		case MapTask:
			// Process map task
			fmt.Printf("Worker received Map task %d\n", reply.Task.Index)
			reply.Task.Output = doMap(reply.Task, mapf)
		case ReduceTask:
			// Process reduce task
			fmt.Printf("Worker received Reduce task %d\n", reply.Task.Index)
			doReduce(reply.Task, reducef)
		default:
			// Unknown task type
			log.Fatalf("Unknown task type received: %v", reply.Task.Type)
		}

		doneAargs := TaskDoneArgs{
			Task: reply.Task,
		}
		doneReply := TaskDoneReply{}
		call("Coordinator.TaskDone", &doneAargs, &doneReply)
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue) []string {
	filepath := task.Inputfile[0]
	content, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatalf("ReadFile %s failed: %v", filepath, err)
	}
	contents := make([][]string, task.NReduce)
	res := mapf(filepath, string(content))
	for _, kv := range res {
		hash := ihash(kv.Key)
		reduce_index := hash % task.NReduce
		line := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		contents[reduce_index] = append(contents[reduce_index], line)
	}
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-map-%d-reduce-%d", task.Index, i)
		lines := contents[i]
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("OpenFile %s failed: %v", filename, err)
		}
		defer file.Close()
		for _, line := range lines {
			_, err = fmt.Fprintf(file, line)
			if err != nil {
				log.Fatalf("WriteString to file %s failed: %v", filename, err)
			}
		}
		task.Output = append(task.Output, filename)
	}
	return task.Output
}

//读取输入文件内容（reply.Task.InputFiles[0]）。
//调用mapf(filename, content)得到[]KeyValue（中间结果）。
//对每个KeyValue，按hash(key) % NReduce确定归属的 reduce 分区，写入对应临时文件。
//生成中间文件路径（例如mr-map-0-1表示 map0 的 reduce1 分区），汇报给 master。

func doReduce(task Task, reducef func(string, []string) string) {
	filepaths := task.Inputfile
	for _, filepath := range filepaths {
		log.Println("%v", filepath)
		//content, err := os.ReadFile(filepath)
		//if err != nil {
		//	log.Fatalf("ReadFile %s failed: %v", filepath, err)
		//}
		//for _, line := range strings.Split(string(content), "\n") {
		//	println(line)
		//}
	}
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
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
