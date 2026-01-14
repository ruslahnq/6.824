package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// each worker will have its own client
type rpcClient struct {
	workerID int
}
var client rpcClient

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// Personal side notes:
// call automatically binded to the master socket
// so it's enough to call this function with the rpcname, args and reply only.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (client *rpcClient) register() {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}

	ok := call("Master.RegisterWorker", &args, &reply)
	if !ok {
		// if call failed then it means master is out which we means we can complete our job
		log.Fatalf("register: RPC call failed. Assuming master is dead.")
	}

	client.workerID = reply.WorkerID
}

func (client *rpcClient) checkDone() bool {
	args := CheckDoneArgs{}
	reply := CheckDoneReply{}

	ok := call("Master.CheckDone", &args, &reply)
	if !ok {
		// if call failed then it means master is out which we means we can complete our job
		log.Fatalf("checkDone: RPC call failed. Assuming master is dead.")
	}

	// however if true is returned then gracefull shutdown will happen
	return reply.Done
}

// returns either mapTask or reduceTask
func (client *rpcClient) getTask() any {
	args := GetTaskArgs{client.workerID}
	reply := GetTaskReply{}

	ok := call("Master.GetTask", &args, &reply)
	if !ok {
		log.Println("getTask: RPC call failed")
		return nil
	}

	if reply.NoTask {
		return nil
	}

	switch reply.TaskType {
	case "Map":
		return mapTask{
			reply.TaskNumber,
			reply.Input[0],
			reply.NReduce,
		}
	case "Reduce":
		return reduceTask{
			reply.TaskNumber,
			reply.Input,
		}
	default:
		return nil
	}
}

func (client *rpcClient) notifyMapTaskCompleted(mtask mapTask, intermediate_files []string, intermediate_files_rids []int) {
	args := NotifyMapTaskCompletedArgs{
		mtask.taskNumber,
		client.workerID,
		intermediate_files,
		intermediate_files_rids,
	}
	reply := NotifyMapTaskCompletedReply{}

	ok := call("Master.NotifyMapTaskCompleted", &args, &reply)
	if !ok {
		log.Println("notifyMapTaskCompleted: RPC call failed")
	}
}

func (client *rpcClient) notifyReduceTaskCompleted(rtask reduceTask, output_file string) {
	args := NotifyReduceTaskCompletedArgs{
		rtask.taskNumber,
		client.workerID,
		output_file,
	}
	reply := NotifyReduceTaskCompletedReply{}

	ok := call("Master.NotifyReduceTaskCompleted", &args, &reply)
	if !ok {
		log.Println("notifyReduceTaskCompleted: RPC call failed")
	}
}

func (client *rpcClient) notifyMapTaskFailed(mtask mapTask) {
	args := NotifyMapTaskFailedArgs{
		mtask.taskNumber,
		client.workerID,
	}
	reply := NotifyMapTaskFailedReply{}

	ok := call("Master.NotifyMapTaskFailed", &args, &reply)
	if !ok {
		log.Println("notifyMapTaskFailed: RPC call failed")
	}
}

func (client *rpcClient) notifyReduceTaskFailed(mtask reduceTask) {
	args := NotifyReduceTaskFailedArgs{
		mtask.taskNumber,
		client.workerID,
	}
	reply := NotifyReduceTaskFailedReply{}

	ok := call("Master.NotifyReduceTaskFailed", &args, &reply)
	if !ok {
		log.Println("notifyReduceTaskFailed: RPC call failed")
	}
}

// peridocally pings master that it's still alive
// should be called in another worker thread
func (client *rpcClient) pingMaster() {
	args := PingArgs{client.workerID}
	reply := PingReply{}

	ok := call("Master.Ping", &args, &reply)
	if !ok {
		log.Println("pingMaster: RPC call failed")
	}
}


// pings peridically (1s) the master about its aliveness
func (client *rpcClient) pinger() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		client.pingMaster()
	}
}

//
// main/mrworker.go calls this function.
//
// Peronsal side notes:
// This Worker already knows about the job it is created for.
// Even if excessive amount of workers have been created,
// and this is one is one of them, then when calling the master for a task,
// it will indifintely wait then it means job is done and it can exit.
//
// However, it can do multiple tasks in its lifetime. 
// So it will keep asking the master for tasks,
// until the master tells it to exit.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.SetOutput(io.Discard)
	client = rpcClient{}
	client.register()
	go client.pinger()

	// checking whether job is completed
	for {
		done := client.checkDone()
		if done {
			break
		}

		// not completed, then asking the master for a task
		task := client.getTask()

		// failed to get a task, continue
		if task == nil {
			// sleeping to reduce overhead of possibly indefinite calling of checkDone() cuz of absence of any task
			log.Println("No Task! Happy sleeping!")
			time.Sleep(1 * time.Second)
			continue
		}

		// actually starting to work on the task
		switch task := task.(type) {
		case mapTask:
			log.Printf("Received Map Task: %v", task.taskNumber)
			intermediate_files, intermediate_files_rids, err := mapWorker(mapf, task)
			if err == nil {
				log.Printf("Done Map Task: %v", task.taskNumber)
				client.notifyMapTaskCompleted(task, intermediate_files, intermediate_files_rids)
			} else {
				log.Println(err)
				client.notifyMapTaskFailed(task)
			}
		case reduceTask:
			log.Printf("Received Reduce Task: %v", task.taskNumber)
			oname, err := reduceWorker(reducef, task)
			if err == nil {
				log.Printf("Done Reduce Task: %v", task.taskNumber)
				client.notifyReduceTaskCompleted(task, oname)
			} else {
				log.Println(err)
				client.notifyReduceTaskFailed(task)
			}
		}
	}
}

type mapTask struct {
	taskNumber int
	splitFile string
	nreduce int
}

func mapWorker(mapf func(string, string) []KeyValue, mtask mapTask) ([]string, []int, error) {
	file, err := os.Open(mtask.splitFile)
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("cannot open file: %v", mtask.splitFile))
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("cannot read file: %v", mtask.splitFile))
	}
	file.Close()
	intermediate := mapf(mtask.splitFile, string(content))

	// DONE: clearing files before writing to them
	// For now, just assumes no such file exists or that it's empty at least
	// TODO: open files beforehead too to reduce the overhead of opening/closing each time while writing
	// open_files := map[string]os.File{}

	for rid := range mtask.nreduce {
		filename := fmt.Sprintf("intermediates/mr-%v-%v", mtask.taskNumber, rid)
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			log.Printf("failed to remove %s: %v", filename, err)
		}
	}

	intermediate_files_mp := make(map[string]int)
	for _, kv := range intermediate {
		rtask_number := ihash(kv.Key) % mtask.nreduce

		filename := fmt.Sprintf("intermediates/mr-%v-%v", mtask.taskNumber, rtask_number)
		intermediate_files_mp[filename] = rtask_number

		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			log.Println(err)
			return nil, nil, errors.New(fmt.Sprintf("cannot open file: %v", filename))
		}

		kvs := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		if _, err := file.WriteString(kvs); err != nil {
			log.Println(err)
			return nil, nil, errors.New(fmt.Sprintf("cannot write file: %v", filename))
		}

		file.Close()
	}

	// each index directly corresponds to one reduce task
	// order matters
	intermediate_files := []string{}
	intermediate_files_rids := []int{}
	for filename, rid := range intermediate_files_mp {
		intermediate_files = append(intermediate_files, filename)
		intermediate_files_rids = append(intermediate_files_rids, rid)
	}

	return intermediate_files, intermediate_files_rids, nil
}

type reduceTask struct {
	taskNumber int
	intermediateFiles []string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceWorker(reducef func(string, []string) string, rtask reduceTask) (string, error) {
	kvs := []KeyValue{}

	for _, filename := range rtask.intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			return "", errors.New(fmt.Sprintf("cannot open file: %v", filename))
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			parts := strings.Fields(line)
			kvs = append(kvs, KeyValue{parts[0], parts[1]})
		}

		file.Close()
	}

	sort.Sort(ByKey(kvs))

	oname := fmt.Sprintf("mr-out-%v", rtask.taskNumber)
	os.Remove(oname)
	ofile, _ := os.Create(oname)
	
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	ofile.Close()

	return oname, nil
}

