package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

const (
	Idle        = "idle"
	Progressing = "progressing"
	Complete    = "completed"
)

type GetTaskReq struct {
	workerID int
}

type GetTaskResp struct {
	taskID        int
	taskType      string
	file          string
	mapTaskNum    int
	reduceTaskNum int
	nReduce       int
}

type ReportTaskReq struct {
	workerID int
	taskID   int
	taskType string
	status   string
}

type nReportTaskResp struct {
	ok bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
