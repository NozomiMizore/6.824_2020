package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type SortedKV []KeyValue

func (KV SortedKV) Len() int {
	return len(KV)
}

func (KV SortedKV) Swap(i, j int) {
	KV[i], KV[j] = KV[j], KV[i]
}

func (KV SortedKV) Less(i, j int) bool {
	return KV[i].Key < KV[j].Key
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	roundFlag := true
	for roundFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, &task)
			callDone(&task)
		case WaittingTask:
			time.Sleep(time.Second)
		case ReduceTask:
			DoReduceTask(reducef, &task)
			callDone(&task)
		case ExitTask:
			fmt.Println("All tasks have been done, will be exiting soon")
			time.Sleep(time.Second)
			roundFlag = false
		}
	}
	time.Sleep(time.Second)
}

//通过rpc获取任务
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Master.PollTask", &args, &reply)
	if ok {
		fmt.Println("worker get a", reply.TaskType, "task, id:", reply.TaskID)
	} else {
		fmt.Println("call for get a task failed!")
	}
	return reply
}

//执行Map任务
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	fileName := response.FileSlice[0]

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	intermediate := mapf(fileName, string(content))
	reducerNum := response.ReducerNum
	HashedKV := make([][]KeyValue, reducerNum)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%reducerNum] = append(HashedKV[ihash(kv.Key)%reducerNum], kv)
	}
	for i := 0; i < reducerNum; i++ {
		tmpName := "mr-tmp-" + strconv.Itoa(response.TaskID) + "-" + strconv.Itoa(i)
		tmp, _ := os.Create(tmpName)
		defer tmp.Close()
		encoder := json.NewEncoder(tmp)
		for _, kv := range HashedKV[i] {
			err := encoder.Encode(kv)
			if err != nil {
				log.Fatalf("error happends when encoding to %v", tmpName)
				return
			}
		}
	}
}

// 读取所得KV对按Key值进行排序
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKV(kva))
	return kva
}

//执行reduce任务
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()

	//完全写入后进行重命名
	fileName := fmt.Sprintf("mr-out-%d", response.TaskID)
	os.Rename(tempFile.Name(), fileName)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

//通过rpc告知Master当前任务已经执行完毕
func callDone(task *Task) Task {
	args := task
	reply := Task{}
	if ok := call("Master.MarkFinished", &args, &reply); ok {
		fmt.Println("worker: task[", args.TaskID, "] has finished!")
	} else {
		fmt.Println("call for markfinished failed!")
	}
	return reply
}
