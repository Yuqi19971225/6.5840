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
	files             []string      // input files
	intermediateFiles [][]string    // intermdediate[mapTaskIndex][reduceTaskIndex] = filename
	allDone           bool
	mapTaskStart      map[int]time.Time // mapTaskStart[mapTaskIndex] = timestamp
	reduceTaskStart   map[int]time.Time // reduceTaskStart[reduceTaskIndex] = timestamp
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
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
	Output    []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	for i := range c.mapTasks {
		if *c.mapTasks[i] == Idle {
			*c.mapTasks[i] = InProgress
			c.mapTaskStart[i] = time.Now()
			reply.Task = Task{
				Type:      MapTask,
				Index:     i,
				Inputfile: []string{c.files[i]},
				NMap:      c.nMap,
				NReduce:   c.nReduce,
			}
			c.mu.Unlock()
			return nil
		}
	}
	for i := range c.reduceTasks {
		if *c.reduceTasks[i] == Idle {
			*c.reduceTasks[i] = InProgress
			c.reduceTaskStart[i] = time.Now()
			reply.Task = Task{
				Type:    ReduceTask,
				Index:   i,
				NMap:    c.nMap,
				NReduce: c.nReduce,
			}
			c.mu.Unlock()
			return nil
		}
	}
	reply.allDone = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	index := args.Task.Index
	taskType := args.Task.Type
	switch taskType {
	case MapTask:
		*c.mapTasks[index] = Completed
		c.mu.Unlock()
		return nil
	case ReduceTask:
		*c.reduceTasks[index] = Completed
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
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
	for i := range c.mapTasks {
		if *c.mapTasks[i] != Completed {
			return ret
		}
	}
	for i := range c.reduceTasks {
		if *c.reduceTasks[i] != Completed {
			return ret
		}
	}
	c.allDone = true
	ret = c.allDone
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
