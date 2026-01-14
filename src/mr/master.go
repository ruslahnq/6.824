package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int
const (
	IDLE TaskStatus = iota
	INPROGRESS
	COMPLETED
)

type MapTaskState struct {
	TaskID int
	Status TaskStatus
	// TODO: implement on top of WorkerID some failure detection, for now non-used
	// Worker get its own WorkerID registering for the master and later will use it
	// for pinging master that it's still alive
	// If master detects no signs of life in the last 10 seconds then set all tasks
	// back to their IDLE state with that WorkerID
	// Doing that we will not store no kind of available workers as workers contact on their own
	// with their respective IDs given from master earlier
	WorkerID int
	SplitFile string // input file for a task
	IntermediateFiles []string // path to intermediate files. len = # of reduce tasks
}

type ReduceTaskState struct {
	TaskID int
	Status TaskStatus
	WorkerID int
	IntermediateFiles []string // path to intermediate files. len = # of map tasks
	OutputFile string // will be path to a output file
}

type Master struct {
	mu sync.Mutex

	NMap int
	NReduce int
	completedReduceTasks int // initially will be 0

	MapTasks []MapTaskState
	ReduceTasks []ReduceTaskState

	workerCounter int
	// map of registered workers with their latest timestamp
	registeredWorkers map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("RegisterWorker | new WorkerID: %v\n", m.workerCounter)

	reply.WorkerID = m.workerCounter

	now := time.Now()
	m.registeredWorkers[m.workerCounter] = now

	m.workerCounter++

	return nil
}

func (m *Master) Ping(args *PingArgs, reply *PingReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.registeredWorkers[args.WorkerID] = now
	return nil
}

// checks registered workers whether they are alive
// if detected that some of them are killed then reset back the tasks
// identified by them
//
// Should be called periodically in another master thread
func (m *Master) checkAlive() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	dead_workers := []int{}

	for worker_id, last_time := range m.registeredWorkers {
		elapsed := now.Sub(last_time)
		if elapsed > 10*time.Second {
			// no ping in past 10 seconds -> means worker is dead
			dead_workers = append(dead_workers, worker_id)
		}
	}

	log.Println("Following Dead Workers are found: ", dead_workers)
	// logging registered workers with their times
	log.Println("\n******************************")
	for wid, last_time := range m.registeredWorkers {
		log.Printf("wid: %v, last_time: %v", wid, last_time)
	}
	log.Println("******************************\n")

	containsf := func(key int, slice []int) bool {
		for _, item := range slice {
			if item == key {
				return true
			}
		}
		return false
	}

	// TODO: mtask is being copied.....
	for _, mtask := range m.MapTasks {
		if mtask.Status != INPROGRESS {
			continue
		}

		if containsf(mtask.WorkerID, dead_workers) {
			m.MapTasks[mtask.TaskID].Status = IDLE
			m.MapTasks[mtask.TaskID].WorkerID = -1
			m.MapTasks[mtask.TaskID].IntermediateFiles = []string{} // this already should be empty however
		}
	}

	for _, rtask := range m.ReduceTasks {
		if rtask.Status != INPROGRESS {
			continue
		}
		
		if containsf(rtask.WorkerID, dead_workers) {
			m.ReduceTasks[rtask.TaskID].Status = IDLE
			m.ReduceTasks[rtask.TaskID].WorkerID = -1
			m.ReduceTasks[rtask.TaskID].OutputFile = "" // this already should be empty however 
		}
	}

	// removing dead workers from registeredWorkers
	for _, wid := range dead_workers {
		delete(m.registeredWorkers, wid)
	}
}


// main assignment algorithm which assigns tasks based on the current state of the job
// always loops over the maps first to check whether some are in need of being handled i.e IDLE
// if found then handle it to the worker
// if no found then loop over reduce tasks that are IDLE
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	completed_map_tasks := 0
	for _, mtask := range m.MapTasks {
		if mtask.Status == COMPLETED {
			completed_map_tasks++
		}
		if mtask.Status == IDLE {
			reply.TaskType = "Map"
			reply.NReduce = m.NReduce
			reply.TaskNumber = mtask.TaskID
			reply.Input = []string{mtask.SplitFile}

			// changing task state
			m.MapTasks[mtask.TaskID].WorkerID = args.WorkerID
			m.MapTasks[mtask.TaskID].Status = INPROGRESS

			// logging
			log.Printf("Giving Map Task #%v to Worker #%v", mtask.TaskID, args.WorkerID)

			return nil
		}
	}

	if completed_map_tasks < m.NMap {
		reply.NoTask = true
		return nil
	}

	// no map tasks found
	// continue with reduce tasks
	for _, rtask := range m.ReduceTasks {
		if rtask.Status == IDLE {
			reply.TaskType = "Reduce"
			reply.NReduce = m.NReduce
			reply.TaskNumber = rtask.TaskID
			reply.Input = rtask.IntermediateFiles

			// changing task state
			m.ReduceTasks[rtask.TaskID].WorkerID = args.WorkerID
			m.ReduceTasks[rtask.TaskID].Status = INPROGRESS

			// log.Printf("Giving Reduce Task #%v to Worker #%v", rtask.TaskID, args.WorkerID)

			return nil
		}
	}

	// no reduce task is found
	reply.NoTask = true
	return nil
}

func (m *Master) NotifyMapTaskCompleted(args *NotifyMapTaskCompletedArgs , reply *NotifyMapTaskCompletedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// logging
	log.Printf("Done Map Task #%v from Worker #%v", args.TaskID, args.WorkerID)

	m.MapTasks[args.TaskID].Status = COMPLETED
	m.MapTasks[args.TaskID].IntermediateFiles = args.IntermediateFiles

	// propagating intermediate files to reduce tasks
	idx := 0
	for _, file := range args.IntermediateFiles {
		rid := args.IntermediateFilesRIDs[idx]
		idx++

		m.ReduceTasks[rid].IntermediateFiles = append(m.ReduceTasks[rid].IntermediateFiles, file)
	}

	return nil
}

func (m *Master) NotifyMapTaskFailed(args *NotifyMapTaskFailedArgs, reply *NotifyMapTaskFailedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// logging
	// log.Printf("Failed Map Task #%v from Worker #%v", args.TaskID, args.WorkerID)

	m.MapTasks[args.TaskID].Status = IDLE
	m.MapTasks[args.TaskID].WorkerID = -1

	return nil
}

func (m *Master) NotifyReduceTaskCompleted(args *NotifyReduceTaskCompletedArgs, reply *NotifyReduceTaskCompletedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// logging
	log.Printf("Done Reduce Task #%v from Worker #%v", args.TaskID, args.WorkerID)


	m.ReduceTasks[args.TaskID].Status = COMPLETED
	m.ReduceTasks[args.TaskID].OutputFile = args.OutputFile

	// NOTE: assuming for any reduce task only single notify will be triggered
	m.completedReduceTasks++

	return nil
}

func (m *Master) NotifyReduceTaskFailed(args *NotifyMapTaskFailedArgs, reply *NotifyMapTaskFailedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// logging
	log.Printf("Failed Reduce Task #%v from Worker #%v", args.TaskID, args.WorkerID)

	m.ReduceTasks[args.TaskID].Status = IDLE
	m.ReduceTasks[args.TaskID].WorkerID = -1

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Println("Master server started")
	go http.Serve(l, nil)
}


// checks workers whether they are alive every 10 second
func (m *Master) aliver() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkAlive()
	}
}

func (m *Master) CheckDone(args *CheckDoneArgs, reply *CheckDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	reply.Done = (m.completedReduceTasks == m.NReduce)

	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.completedReduceTasks == m.NReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.SetOutput(io.Discard)

	m := Master{}

	// Your code here.
	os.RemoveAll("intermediates")
	os.Mkdir("intermediates", 0755)
	// os.RemoveAll("output")
	// os.Mkdir("output", 0755)

	m.NMap = len(files)
	m.NReduce = nReduce
	m.completedReduceTasks = 0
	m.workerCounter = 0
	m.registeredWorkers = make(map[int]time.Time)

	// initializing tasks
	for mid, split_file := range files {
		m.MapTasks = append(m.MapTasks, MapTaskState{
			TaskID: mid,
			Status: IDLE,
			WorkerID: -1,
			SplitFile: split_file,
			IntermediateFiles: []string{},
		})
	}
	for rid := range nReduce {
		m.ReduceTasks = append(m.ReduceTasks, ReduceTaskState{
			TaskID: rid,
			Status: IDLE,
			WorkerID: -1,
			IntermediateFiles: []string{},
			OutputFile: "",
		})
	}

	go m.aliver()
	m.server()

	return &m
}
