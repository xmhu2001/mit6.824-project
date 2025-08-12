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

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	ExitTask   = "exit"
	WaitTask   = "wait"
)

const (
	Idle        = "idle"
	Progressing = "progressing"
	Completed   = "Completed"
)

type GetTaskReq struct {
	WorkerID int
}

type GetTaskResp struct {
	TaskID        int
	TaskType      string
	File          string
	MapTaskNum    int
	ReduceTaskNum int
	NReduce       int
}

type ReportTaskReq struct {
	WorkerID  int
	TaskID    int
	TaskType  string
	Completed bool
}

type ReportTaskResp struct {
	OK bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
