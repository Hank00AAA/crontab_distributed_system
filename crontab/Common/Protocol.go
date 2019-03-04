package Common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定时任务
type Job struct {
	Name string `json:"name"`
	Command string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

//任务调度计划
type JobSchedulerPlan struct{
	Job *Job //要调度的任务信息
	Expr *cronexpr.Expression //解析好的cronexpr
	NextTime time.Time //下次调度时间
}

//任务执行状态
type JobExecuteInfo struct{
	Job *Job //任务信息
	PlanTime time.Time //理论上的调度时间
	RealTime time.Time //实际调度时间
	CancelCtx context.Context //用于取消任务
	CancelFunc context.CancelFunc //用于取消command执行的函数
}

//HTTP接口应答
type Response struct{
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

type JobEvent struct {
	EventType int // SAVE, DELETE
	Job *Job
}

//任务执行结果
type JobExecuteResult struct{
	ExecuteInfo *JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

//任务执行日志
type JobLog struct{
	JobName string `bson:"jobName" json:"jobName"`
	Command string `bson:"command" json:"command"`
	Err     string `bson:"err" json:"err"`
	Output  string `bson:"output" json:"output"`
	PlanTime int64 `bson:"planTime" json:"planTime"`
	SchedulerTime int64 `bson:"scheduleTime" json:"scheduleTime"`
	StartTime int64 `bson:"startTime" json:"startTime"`
	EndTime   int64 `bson:"endTime" json:"endTime"`
}

type LogBatch struct{
	Logs []interface{} //多条日志
}

//任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//任务日志排序规则
type SortLogByStarTime struct{
	SortOrder int `bson:"startTime"` //{startTime:-1}
}


//反序列化job
func UnpackJob(value []byte)(ret *Job, err error){
	var(
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job);err!=nil{
		return
	}
	ret = job
	return
}

func BuildResponse(errno int, msg string, data interface{})(resp []byte, err error){

	//1. 定义response
	var(
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	//2. 序列化
	resp, err = json.Marshal(response)

	return
}

//从etcd的key中提取任务名
///cron/jobs/job10 -> job10
func ExtractJobName(jobkey string)(string){
	return strings.TrimPrefix(jobkey, JOB_SAVE)
}

//从etcd的key中提取任务名
///cron/killer/job10 -> job10
func ExtractKillerName(jobkey string)(string){
	return strings.TrimPrefix(jobkey, JOB_KILLER_DIR)
}

//任务变化事件有两种：1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job)(jobEvent *JobEvent){
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

//构造执行计划
func BuildJobSchedulePlan(job *Job)(jobSchedulerPlan *JobSchedulerPlan, err error){
	var(
		expr *cronexpr.Expression
	)

	//解析cron表达式
	if expr ,err = cronexpr.Parse(job.CronExpr);err!=nil{
		return
	}

	//生成任务调度计划
	jobSchedulerPlan = &JobSchedulerPlan{
		Job: job,
		Expr:expr,
		NextTime:expr.Next(time.Now()),
	}
	return
}

//构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulerPlan)(jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job: jobSchedulePlan.Job,
		PlanTime:jobSchedulePlan.NextTime, //计划调度时间
		RealTime:time.Now(),//真实调度时间
	}

	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return
}

//提取workerIP
func ExtractWorkerIP(regKey string)(string){
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}

