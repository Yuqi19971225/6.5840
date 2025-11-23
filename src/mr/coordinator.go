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
	defer c.mu.Unlock()

	// 检查是否所有任务都已完成
	if c.allDone {
		reply.Task = Task{}
		reply.allDone = true
		return nil
	}

	// 检查是否所有 Map 任务已完成
	allMapDone := true
	for i := range c.mapTasks {
		if *c.mapTasks[i] != Completed {
			allMapDone = false
			break
		}
	}

	// 优先分配 Reduce 任务（如果 Map 任务已完成）
	if allMapDone {
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
				// 收集该 Reduce 任务需要的中间文件
				for i := 0; i < c.nMap; i++ {
					reply.Task.Inputfile = append(reply.Task.Inputfile, c.intermediateFiles[i][j])
				}
				return nil
			}
		}
	} else {
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
					Output:    make([]string, 0),
				}
				return nil
			}
		}
	}

	// 如果没有空闲任务，返回 allDone 状态
	reply.allDone = c.Done()
	if !reply.allDone {
		reply.Task = Task{} // 返回空任务，让 worker 稍后重试
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	index := args.Task.Index
	taskType := args.Task.Type
	output := args.Task.Output

	switch taskType {
	case MapTask:
		if index < 0 || index >= len(c.mapTasks) {
			log.Printf("Invalid map task index: %d", index)
			return nil
		}

		*c.mapTasks[index] = Completed

		// 存储中间文件
		if len(output) == c.nReduce {
			for i := 0; i < c.nReduce; i++ {
				c.intermediateFiles[index][i] = output[i]
			}
		}

	case ReduceTask:
		if index < 0 || index >= len(c.reduceTasks) {
			log.Printf("Invalid reduce task index: %d", index)
			return nil
		}

		*c.reduceTasks[index] = Completed
	}

	// 检查是否所有任务都已完成
	c.Done()

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

	for i := range c.mapTasks {
		if *c.mapTasks[i] != Completed {
			return false
		}
	}
	for i := range c.reduceTasks {
		if *c.reduceTasks[i] != Completed {
			return false
		}
	}

	// 设置 allDone 标志
	c.allDone = true
	log.Println("All tasks completed")
	return true
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
