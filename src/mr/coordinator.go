package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	nMap              int           // number of map tasks
	nReduce           int           // number of reduce tasks
	mapTasks          []*TaskStatus // "idle", "in-progress", "completed"
	reduceTasks       []*TaskStatus // "idle", "in-progress", "completed"
	intermediateFiles [][]string    // intermdediate[mapTaskIndex][reduceTaskIndex] = filename
	alldDone          bool
	mapTaskStart      map[int]time.Time // mapTaskStart[mapTaskIndex] = timestamp
	reduceTaskStart   map[int]time.Time // reduceTaskStart[reduceTaskIndex] = timestamp
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	DoneTask
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Type      TaskType
	Index     int      // map task index or reduce task index
	Inputfile []string // for map task
	NMap      int      // number of map tasks
	NReduce   int      // number of reduce tasks
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
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
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]*TaskStatus, c.nMap)
	c.reduceTasks = make([]*TaskStatus, c.nReduce)
	c.intermediateFiles = make([][]string, c.nMap)
	c.mapTaskStart = make(map[int]time.Time)
	c.reduceTaskStart = make(map[int]time.Time)

	for i := range c.mapTasks {
		c.mapTasks[i] = new(TaskStatus)
		*c.mapTasks[i] = Idle
		c.intermediateFiles[i] = make([]string, c.nReduce)
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = new(TaskStatus)
		*c.reduceTasks[i] = Idle
	}

	c.server()
	return &c
}
