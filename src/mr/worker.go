package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		res := call("Coordinator.GetTask", &args, &reply)
		if res == false {
			// 不要终止 worker，可能只是协调器暂时不可用
			log.Printf("GetTask RPC call failed, waiting and retrying...")
			time.Sleep(1 * time.Second) // 等待一秒后重试
			continue
		}

		// 检查任务类型
		switch reply.Task.Type {
		case MapTask:
			// Process map task
			fmt.Printf("Worker received Map task %d\n", reply.Task.Index)
			doMap(reply.Task, mapf)

			// 通知协调器任务完成
			doneArgs := TaskDoneArgs{
				Task: Task{
					Type:      MapTask,
					Index:     reply.Task.Index,
					Inputfile: reply.Task.Inputfile,
					NMap:      reply.Task.NMap,
					NReduce:   reply.Task.NReduce,
				},
			}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)

		case ReduceTask:
			// Process reduce task
			fmt.Printf("Worker received Reduce task %d\n", reply.Task.Index)
			doReduce(reply.Task, reducef)

			// 通知协调器任务完成
			doneArgs := TaskDoneArgs{
				Task: Task{
					Type:      ReduceTask,
					Index:     reply.Task.Index,
					Inputfile: reply.Task.Inputfile,
					NMap:      reply.Task.NMap,
					NReduce:   reply.Task.NReduce,
				},
			}
			doneReply := TaskDoneReply{}
			call("Coordinator.TaskDone", &doneArgs, &doneReply)

		case NoneTask:
			// 没有可用任务，检查是否完成
			time.Sleep(100 * time.Millisecond)
			continue

		default:
			// 未知任务类型，等待
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	if len(task.Inputfile) == 0 {
		return
	}
	filepath := task.Inputfile[0]
	content, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatalf("ReadFile %s failed: %v", filepath, err)
	}

	// 创建临时存储，每个 reduce 任务一个字符串切片
	tempContents := make([][]string, task.NReduce)
	res := mapf(filepath, string(content))

	// 按 hash 值分组
	for _, kv := range res {
		hash := ihash(kv.Key)
		reduceIndex := hash % task.NReduce
		line := fmt.Sprintf("%v %v", kv.Key, kv.Value)
		tempContents[reduceIndex] = append(tempContents[reduceIndex], line)
	}

	// 为每个 reduce 任务创建输出文件
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.Index, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Create file %s failed: %v", filename, err)
		}

		writer := bufio.NewWriter(file)
		for _, line := range tempContents[i] {
			_, err = writer.WriteString(line + "\n")
			if err != nil {
				log.Fatalf("Write to file %s failed: %v", filename, err)
			}
		}

		err = writer.Flush()
		if err != nil {
			log.Fatalf("Flush file %s failed: %v", filename, err)
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("Close file %s failed: %v", filename, err)
		}
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	// 构建中间文件名
	filepaths := make([]string, 0, task.NMap)
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.Index)
		filepaths = append(filepaths, filename)
	}

	var kvs []KeyValue

	// 读取所有输入文件
	for _, filepath := range filepaths {
		file, err := os.Open(filepath)
		if err != nil {
			log.Printf("Open file %s failed: %v", filepath, err)
			continue // 继续处理其他文件
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			parts := strings.SplitN(line, " ", 2)
			if len(parts) == 2 {
				kvs = append(kvs, KeyValue{parts[0], parts[1]})
			}
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("Close file %s failed: %v", filepath, err)
		}
	}

	// 按键排序
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// 创建输出文件
	outputFile := fmt.Sprintf("mr-out-%d", task.Index)
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Create output file %s failed: %v", outputFile, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// 执行 reduce 操作
	i := 0
	for i < len(kvs) {
		j := i + 1
		// 找到所有相同键的项
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}

		// 收集相同键的值
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}

		// 执行 reduce 函数
		result := reducef(kvs[i].Key, values)

		// 写入结果
		_, err := fmt.Fprintf(writer, "%v %v\n", kvs[i].Key, result)
		if err != nil {
			log.Fatalf("Write result to file failed: %v", err)
		}

		i = j
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// 注意：这里改为 log.Printf 而不是 log.Fatal，避免 worker 退出
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("RPC call error: %v", err)
	return false
}
