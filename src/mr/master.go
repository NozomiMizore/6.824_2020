package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Master全局锁，使得work对Master顺序访问
var mutex sync.Mutex

type Master struct {
	// Your definitions here.
	ReducerNum        int         //reducer数目
	TaskID            int         //task对应id
	SysPhase          Phase       //框架当前所处阶段
	MapTaskChannel    chan *Task  //Map任务队列
	ReduceTaskChannel chan *Task  //Reduce任务队列
	TaskMetaMap       TaskMetaMap //全部任务元数据Map
	FileSlice         []string    //输入文件对应切片
}

//任务元数据
type TaskMetaInfo struct {
	TaskState State     //任务状态
	StartTime time.Time //任务开始时间
	TaskAddr  *Task     //任务指针
}

//Map结构保存全部任务元数据
type TaskMetaMap struct {
	MetaMap map[int]*TaskMetaInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

func (m *Master) MarkFinished(args *Task, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := m.TaskMetaMap.MetaMap[args.TaskID]
		if ok && meta.TaskState == Working {
			meta.TaskState = Done
		} else {
			fmt.Printf("Map task[%d] is already finished\n", args.TaskID)
		}
	case ReduceTask:
		meta, ok := m.TaskMetaMap.MetaMap[args.TaskID]
		if ok && meta.TaskState == Working {
			meta.TaskState = Done
		} else {
			fmt.Printf("Reduce task[%d] is already finished\n", args.TaskID)
		}
	default:
		panic("undefined task type!")
	}
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	if m.SysPhase == AllDone {
		fmt.Printf("All tasks have been finished, Master will be exiting soon!")
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		FileSlice:         files,
		ReducerNum:        nReduce,
		SysPhase:          MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskMetaMap:       TaskMetaMap{make(map[int]*TaskMetaInfo, len(files)+nReduce)},
		TaskID:            0,
	}

	// Your code here.
	m.initMapTasks(files)

	m.server()

	go m.CrashDetector()

	return &m
}

//TaskMetaMap接收任务元数据
func (t *TaskMetaMap) accept(Info *TaskMetaInfo) bool {
	id := Info.TaskAddr.TaskID
	meta := t.MetaMap[id]
	if meta != nil {
		return false
	} else {
		t.MetaMap[id] = Info
	}
	return true
}

//检查任务完成情况,判断是否需要推进到下一阶段
func (t *TaskMetaMap) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskAddr.TaskType == MapTask {
			if v.TaskState == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAddr.TaskType == ReduceTask {
			if v.TaskState == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	//判断是否需要推进到下一阶段
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}

//判断指定任务是否在工作，不在工作则开始工作返回true
func (t *TaskMetaMap) judgeState(taskID int) bool {
	taskInfo, ok := t.MetaMap[taskID]
	if !ok || taskInfo.TaskState != Waiting {
		return false
	}
	taskInfo.TaskState = Working
	taskInfo.StartTime = time.Now()
	return true

}

//根据reduce序号获取对应的中间文件名切片
func getTmpName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// 产生task唯一id
func (m *Master) generateTaskID() int {
	res := m.TaskID
	m.TaskID++
	return res
}

//检测发生crash的task并进行对应处理
func (m *Master) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mutex.Lock()
		if m.SysPhase == AllDone {
			mutex.Unlock()
			break
		}
		for _, v := range m.TaskMetaMap.MetaMap {
			if v.TaskState == Working {
				fmt.Println("task[", v.TaskAddr.TaskID, "] is working", time.Since(v.StartTime), "s")
			}
			if v.TaskState == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[%d] is crashed, take [%d] s\n", v.TaskAddr.TaskID, time.Since(v.StartTime)/time.Second)
				switch v.TaskAddr.TaskType {
				case MapTask:
					m.MapTaskChannel <- v.TaskAddr
					v.TaskState = Waiting
				case ReduceTask:
					m.ReduceTaskChannel <- v.TaskAddr
					v.TaskState = Waiting
				}
			}
		}
		mutex.Unlock()
	}
}

// 初始化map任务
func (m *Master) initMapTasks(files []string) {
	for _, v := range files {
		id := m.generateTaskID()
		task := Task{
			TaskType:   MapTask,
			TaskID:     id,
			ReducerNum: m.ReducerNum,
			FileSlice:  []string{v},
		}

		TaskMetaInfo := TaskMetaInfo{
			TaskState: Waiting,
			TaskAddr:  &task,
		}
		m.TaskMetaMap.accept(&TaskMetaInfo)
		m.MapTaskChannel <- &task
	}
}

//初始化reduce任务
func (m *Master) initReduceTasks() {
	for i := 0; i < m.ReducerNum; i++ {
		id := m.generateTaskID()
		task := Task{
			TaskID:    id,
			TaskType:  ReduceTask,
			FileSlice: getTmpName(i),
		}

		TaskMetaInfo := TaskMetaInfo{
			TaskState: Waiting,
			TaskAddr:  &task,
		}

		m.TaskMetaMap.accept(&TaskMetaInfo)
		m.ReduceTaskChannel <- &task
	}
}

//推进到下一阶段
func (m *Master) toNextPhase() {
	if m.SysPhase == MapPhase {
		m.initReduceTasks()
		m.SysPhase = ReducePhase
	} else if m.SysPhase == ReducePhase {
		m.SysPhase = AllDone
	}
}

//分发任务
func (m *Master) PollTask(args *TaskArgs, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()

	switch m.SysPhase {
	case MapPhase:
		if len(m.MapTaskChannel) > 0 {
			*reply = *<-m.MapTaskChannel
			if !m.TaskMetaMap.judgeState(reply.TaskID) {
				fmt.Printf("Map-task[%d] is running\n", reply.TaskID)
			}
		} else {
			reply.TaskType = WaittingTask
			if m.TaskMetaMap.checkTaskDone() {
				m.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(m.ReduceTaskChannel) > 0 {
			*reply = *<-m.ReduceTaskChannel
			if !m.TaskMetaMap.judgeState(reply.TaskID) {
				fmt.Printf("Reduce-task[%d] is running\n", reply.TaskID)
			}
		} else {
			reply.TaskType = WaittingTask
			if m.TaskMetaMap.checkTaskDone() {
				m.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("Undefined phase!")
	}
	return nil
}
