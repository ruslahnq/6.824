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

type RegisterWorkerArgs struct {}

type RegisterWorkerReply struct {
	WorkerID int
}

type PingArgs struct {
	WorkerID int
}
type PingReply struct {}

type CheckDoneArgs struct {}

type CheckDoneReply struct {
	Done bool
}

type NotifyMapTaskCompletedArgs struct {
	TaskID int
	WorkerID int
	IntermediateFiles []string
	IntermediateFilesRIDs []int
}

type NotifyMapTaskCompletedReply struct {}

type NotifyMapTaskFailedArgs struct {
	TaskID int
	WorkerID int
}

type NotifyMapTaskFailedReply struct {}

type NotifyReduceTaskCompletedArgs struct {
	TaskID int
	WorkerID int
	OutputFile string
}

type NotifyReduceTaskCompletedReply struct {}

type NotifyReduceTaskFailedArgs struct {
	TaskID int
	WorkerID int
}

type NotifyReduceTaskFailedReply struct {}

// Add your RPC definitions here.
type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	NoTask bool
	TaskType    string   // "Map", "Reduce", or "Wait", "Exit"
	TaskNumber  int      // Map or Reduce task number
	NReduce     int      // Number of reduce tasks, essentially needed for map task to know how to take modulo
	Input []string // will consists of single file for a map task, and multiple files for a reduce task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
