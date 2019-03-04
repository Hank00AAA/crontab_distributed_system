package worker

import (
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"time"
)

//任务调度
type Scheduler struct{
	jobEventChan chan *Common.JobEvent // etcd任务事件队列
	jobPlanTable map[string]*Common.JobSchedulerPlan
	jobExecutingTable map[string]*Common.JobExecuteInfo
	jobResultChan chan *Common.JobExecuteResult //任务结果队列
}

var(
	G_scheduler *Scheduler
)

//处理任务事件
func (scheduler *Scheduler)handleJobEvent(jobEvent *Common.JobEvent){

	var(
		jobSchedulerPlan *Common.JobSchedulerPlan
		jobExecuteInfo *Common.JobExecuteInfo
		jobExecuting bool
		err error
		jobExisted bool
	)

	switch jobEvent.EventType{
	case Common.JOB_EVENT_SAVE: //保存任务
		if jobSchedulerPlan, err = Common.BuildJobSchedulePlan(jobEvent.Job);err!=nil{
			return
		}
		G_scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulerPlan
	case Common.JOB_EVENT_DELETE: //删除任务
		if jobSchedulerPlan, jobExisted = G_scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted{
			delete(G_scheduler.jobPlanTable, jobSchedulerPlan.Job.Name)
		}
	case Common.JOB_EVENT_KILLER:
		//取消掉Command执行
		//首先判断任务是否执行中
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExecuting{
			jobExecuteInfo.CancelFunc() //出发command杀死shell子进程
		}
	}
}

func (scheduler *Scheduler)TryStarJob(jobPlan *Common.JobSchedulerPlan){
	//调度和执行是两件事情
	//执行的任务可能运行很久,1分钟调度60次，但只能执行1次

	var(
		jobExecuteInfo *Common.JobExecuteInfo
		jobExecuting bool
	)

	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name];jobExecuting{
		fmt.Println("尚未推出，跳出执行:", jobPlan.Job.Name)
		return
	}

	//构建执行状态
	jobExecuteInfo = Common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	//TODO:
	//fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)


}

//重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule()(schedulerAfter time.Duration){


	var(
		jobPlan *Common.JobSchedulerPlan
		now time.Time
		nearTime *time.Time
	)

	//如果任务表为空，随便睡眠
	if len(G_scheduler.jobPlanTable) == 0{
		schedulerAfter = 1*time.Second
		return
	}

	//当前时间
	now = time.Now()

	//1.遍历所有任务
	for _ , jobPlan = range G_scheduler.jobPlanTable{
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			//TODO:尝试执行
			scheduler.TryStarJob(jobPlan)

			//如果前一次执行没结束 就不会再次启动
			jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次执行时间
		}

		//统计最近的一个过期任务的时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}
	//2. 过期的任务立马执行
	//3. 统计最近要过期的任务的时间(N秒后过期)

	//下次调度时间间隔
	schedulerAfter = (*nearTime).Sub(now)

	return

}

//处理任务结果
func (scheduler *Scheduler)handleJobResult(result *Common.JobExecuteResult){

	var(
		jobLog *Common.JobLog
	)

	//删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	//生成执行日志
	if result.Err!=Common.ERR_LOCK_ALREADY_REQUIRED{
		jobLog = &Common.JobLog{
			JobName:result.ExecuteInfo.Job.Name,
			Command:result.ExecuteInfo.Job.Command,
			Output: string(result.Output),
			PlanTime: result.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
			SchedulerTime:result.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime:result.StartTime.UnixNano()/1000/1000,
			EndTime:result.EndTime.UnixNano()/1000/1000,
		}
		if result.Err!=nil{
			jobLog.Err = result.Err.Error()
		}else{
			jobLog.Err = ""
		}
		G_logSink.Append(jobLog)
	}

	//TODO:存储到MongoDB
	//不能在这里存，一个插入耗时，可能阻塞调度
	//把日志转发到单独的处理模块

}

//调度协程
func(scheduler *Scheduler)schedulerLoop(){

	var(
		jobEvent *Common.JobEvent
		schedulerAfter time.Duration
		schedulerTimer *time.Timer
		jobResult *Common.JobExecuteResult
	)

	//初始化一次
	schedulerAfter = scheduler.TrySchedule()

	//调度的延迟定时器
	schedulerTimer = time.NewTimer(schedulerAfter)


	//定时任务/cron
	for{
		select {
		case jobEvent = <-scheduler.jobEventChan://监听任务变化事件
			//对内存维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <- schedulerTimer.C: //最近的任务到期了
		case jobResult = <- scheduler.jobResultChan:
			scheduler.handleJobResult(jobResult)
		}

		//调度一次
		schedulerAfter = G_scheduler.TrySchedule()

		//重置调度间隔
		schedulerTimer.Stop()
		schedulerTimer.Reset(schedulerAfter)
	}
}

//推送任务变化事件
func (scheduler *Scheduler)PushJobEvent(jobEvent *Common.JobEvent){
	scheduler.jobEventChan <- jobEvent
}

//初始化调度器
func InitScheduler(){
	G_scheduler = &Scheduler{
		jobEventChan:make(chan *Common.JobEvent, 1000),
		jobPlanTable:make(map[string]*Common.JobSchedulerPlan),
		jobExecutingTable:make(map[string]*Common.JobExecuteInfo),
		jobResultChan:make(chan *Common.JobExecuteResult, 1000),
	}

	//启动调度协程
	go G_scheduler.schedulerLoop()

	return
}

//回传任务执行结果
func (scheduler *Scheduler )PushJobResult(jobResult *Common.JobExecuteResult){
	scheduler.jobResultChan <- jobResult

	fmt.Println("任务执行完成：", jobResult.ExecuteInfo.Job.Name, string(jobResult.Output), jobResult.Err)
}
