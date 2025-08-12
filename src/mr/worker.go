package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	workerID := os.Getpid()

	for {
		resp := getTask(workerID)

		switch resp.TaskType {
		case MapTask:
			doMap(resp, mapf, workerID)
		case ReduceTask:
			doReduce(resp, reducef, workerID)
		case WaitTask:
			time.Sleep(1.0 * time.Second)
			continue
		case ExitTask:
			return
		}
	}

}

func doMap(task *GetTaskResp, mapf func(string, string) []KeyValue, workerID int) {

	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))

	// 分桶
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		tempFile, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Fatal("cannot create temp File")
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", task.TaskID, i))
	}
	reportTaskDone(task.TaskType, task.TaskID, workerID)
}

func reportTaskDone(taskType string, taskID int, workerID int) {
	req := &ReportTaskReq{
		WorkerID:  workerID,
		TaskID:    taskID,
		TaskType:  taskType,
		Completed: true,
	}
	resp := &ReportTaskResp{}
	call("Coordinator.ReportTask", req, resp)
	if !resp.OK {
		log.Fatal("report task done failed")
	}
}

func doReduce(task *GetTaskResp, reducef func(string, []string) string, workerID int) {
	// reducer 编号
	reduceTaskNum := task.ReduceTaskNum
	pattern := fmt.Sprintf("mr-*-%d", reduceTaskNum)

	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal("cannot get match File")
	}

	// 收集分到同一个桶的临时文件中的 kv 对
	intermediate := []KeyValue{}
	for i := 0; i < len(matches); i++ {
		filename := matches[i]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// shuffle
	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp("", "mr-out-tmp-*")
	if err != nil {
		log.Fatal("cannot create temp File")
	}
	defer tempFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 格式化写入文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNum))
	reportTaskDone(task.TaskType, task.TaskID, workerID)
}

func getTask(workerID int) (resp *GetTaskResp) {
	req := &GetTaskReq{
		WorkerID: workerID,
	}
	resp = new(GetTaskResp)
	call("Coordinator.GetTask", req, resp)
	return resp
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
