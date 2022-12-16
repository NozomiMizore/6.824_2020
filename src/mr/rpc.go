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

// Add your RPC definitions here.

// 当前框架所处阶段type
type Phase int

// 任务类型type
type Type int

// 任务执行状态type
type State int

// 任务参数用不到 为空
type TaskArgs struct {
}

type Task struct {
	TaskType   Type     //任务类型
	TaskID     int      //任务ID
	ReducerNum int      //reducer数目
	FileSlice  []string //输入文件切片，map任务该项只有一个值，reduce任务则有多个中间文件名作为输入
}

// 枚举当前框架所处阶段
const (
	MapPhase    Phase = iota // 框架当前处于Map阶段
	ReducePhase              // 框架当前处于Reduce阶段
	AllDone                  // 任务已全部完成
)

// 枚举任务类型
const (
	MapTask      Type = iota // 任务为Map类型
	ReduceTask               // 任务为Reduce类型
	WaittingTask             // 任务已经分发完，在等待执行完成(伪任务)
	ExitTask                 // worker可以结束(伪任务)
)

// 枚举任务状态
const (
	Working State = iota //任务正在工作
	Waiting              //任务正在等待
	Done                 //任务已完成
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
