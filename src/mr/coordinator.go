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

const (
	TIMEOUT = 600
)

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	nMap            int           // number of map tasks
	nReduce         int           // number of reduce tasks
	mapTasks        []*TaskStatus // "idle", "in-progress", "completed"
	reduceTasks     []*TaskStatus // "idle", "in-progress", "completed"
	files           []string      // input files
	taskPhase       TaskPhase
	mapTaskStart    map[int]time.Time // mapTaskStart[mapTaskIndex] = timestamp
	reduceTaskStart map[int]time.Time // reduceTaskStart[reduceTaskIndex] = timestamp
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoneTask
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
	DonePhase
)

type Task struct {
	Type      TaskType
	Index     int      // map task index or reduce task index
	Inputfile []string // for map task
	NMap      int      // number of map tasks
	NReduce   int      // number of reduce tasks
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("GetTask", c.taskPhase)

	// 优先分配 Reduce 任务（如果 Map 任务已完成）
	switch c.taskPhase {
	case ReducePhase:
		for j := range c.reduceTasks {
			if *c.reduceTasks[j] == Idle {
				*c.reduceTasks[j] = InProgress
				c.reduceTaskStart[j] = time.Now()
				reply.Task = Task{
					Type:      ReduceTask,
					Index:     j,
					NMap:      c.nMap,
					NReduce:   c.nReduce,
					Inputfile: make([]string, 0),
				}
				return nil
			}
		}
		for j := range c.reduceTasks {
			if *c.reduceTasks[j] == InProgress && time.Since(c.reduceTaskStart[j]).Seconds() > TIMEOUT {
				c.reduceTaskStart[j] = time.Now()
				reply.Task = Task{
					Type:      ReduceTask,
					Index:     j,
					NMap:      c.nMap,
					NReduce:   c.nReduce,
					Inputfile: make([]string, 0),
				}
			}
		}

		allRecudeDone := true
		for i := range c.reduceTasks {
			if *c.reduceTasks[i] != Completed {
				allRecudeDone = false
				break
			}
		}
		if allRecudeDone {
			c.taskPhase = DonePhase
		}
		return nil
	case MapPhase:
		// 分配 Map 任务
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
				return nil
			}
		}
		// 重试超时任务
		for i := range c.mapTasks {
			if *c.mapTasks[i] == InProgress && time.Since(c.mapTaskStart[i]).Seconds() > TIMEOUT {
				c.mapTaskStart[i] = time.Now()
				reply.Task = Task{
					Type:      MapTask,
					Index:     i,
					Inputfile: []string{c.files[i]},
					NMap:      c.nMap,
					NReduce:   c.nReduce,
				}
				return nil
			}
		}
		allMapDone := true
		for i := range c.mapTasks {
			if *c.mapTasks[i] != Completed {
				allMapDone = false
				break
			}
		}
		if allMapDone {
			c.taskPhase = ReducePhase
		}

		reply.Task = Task{
			Type: NoneTask,
		}
		return nil
	default:
		panic("unhandled default case")
	}

	// 如果没有空闲任务，返回 allDone 状态
	//reply.allDone = c.Done()
	//if !reply.allDone {
	//	reply.Task = Task{} // 返回空任务，让 worker 稍后重试
	//}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	index := args.Task.Index
	switch args.Task.Type {
	case MapTask:
		if index < 0 || index >= len(c.mapTasks) {
			log.Printf("Invalid map task index: %d", index)
			return nil
		}
		*c.mapTasks[index] = Completed

	case ReduceTask:
		if index < 0 || index >= len(c.reduceTasks) {
			log.Printf("Invalid reduce task index: %d", index)
			return nil
		}
		*c.reduceTasks[index] = Completed
	}
	log.Println("Coordinator.TaskDone check all done", args.Task.Type, args.Task.Index)
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
	sockname := coordinatorSock()

	// 确保旧的 socket 文件被删除
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Printf("Coordinator server starting on %s", sockname)

	// 启动 HTTP 服务器
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.taskPhase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = files
	c.mapTasks = make([]*TaskStatus, c.nMap)
	c.reduceTasks = make([]*TaskStatus, c.nReduce)
	c.mapTaskStart = make(map[int]time.Time)
	c.reduceTaskStart = make(map[int]time.Time)

	for i := range c.mapTasks {
		c.mapTasks[i] = new(TaskStatus)
		*c.mapTasks[i] = Idle
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = new(TaskStatus)
		*c.reduceTasks[i] = Idle
	}

	c.server()
	return &c
}
