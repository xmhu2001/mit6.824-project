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

type Task struct {
	taskID    int
	status    string
	file      string
	startTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	files       []string
	mapTasks    []Task
	reduceTasks []Task
	mapFinished bool
	allFinished bool
	nReduce     int
}

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

func (c *Coordinator) GetTask(req *GetTaskReq) (resp *GetTaskResp, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// timeout task check
	c.checkTimeout()

	if c.allFinished {
		resp.taskType = ExitTask
		return resp, nil
	}

	// mapTask 还未执行完，分配 mapTask
	if !c.mapFinished {
		for i, task := range c.mapTasks {
			if task.status == Idle {
				resp.taskType = MapTask
				resp.taskID = task.taskID
				resp.file = task.file
				resp.nReduce = c.nReduce

				c.mapTasks[i].status = Progressing
				c.mapTasks[i].startTime = time.Now()
				return resp, nil
			}
		}
		resp.taskType = WaitTask
		return resp, nil
	}

	for i, task := range c.reduceTasks {
		if task.status == Idle {
			resp.taskType = ReduceTask
			resp.taskID = task.taskID
			resp.reduceTaskNum = i
			resp.mapTaskNum = len(c.mapTasks)

			c.reduceTasks[i].status = Progressing
			c.reduceTasks[i].startTime = time.Now()
			return resp, nil
		}
	}
	resp.taskType = WaitTask
	return resp, nil
}

func (c *Coordinator) checkTimeout() {
	timeout := 10 * time.Second
	now := time.Now()

	if !c.mapFinished {
		for i, task := range c.mapTasks {
			if task.status == Progressing && now.Sub(task.startTime) > timeout {
				c.mapTasks[i].status = Idle
			}
		}
	}

	if c.mapFinished && !c.allFinished {
		for i, task := range c.reduceTasks {
			if task.status == Progressing && now.Sub(task.startTime) > timeout {
				c.reduceTasks[i].status = Idle
			}
		}
	}
}

func (c *Coordinator) ReportTask(req *ReportTaskReq) (resp *ReportTaskResp, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if req.taskType == MapTask {
		for i, task := range c.mapTasks {
			if req.taskID == task.taskID && req.status == Progressing {
				c.mapTasks[i].status = Completed

				allCompleted := true
				for _, task := range c.mapTasks {
					if task.status != Completed {
						allCompleted = false
						break
					}
				}
				c.mapFinished = allCompleted
				resp.ok = true
				return resp, nil
			}
		}
	}

	if req.taskType == ReduceTask {
		for i, task := range c.reduceTasks {
			if req.taskID == task.taskID && req.status == Progressing {
				c.reduceTasks[i].status = Completed

				allCompleted := true
				for _, task := range c.reduceTasks {
					if task.status != Completed {
						allCompleted = false
						break
					}
				}
				c.allFinished = allCompleted
				resp.ok = true
				return resp, nil
			}
		}
	}
	resp.ok = false
	return resp, nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			taskID: i,
			status: Idle,
			file:   file,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			taskID: i,
			status: Idle,
		}
	}

	for {

	}

	c.server()
	return &c
}
