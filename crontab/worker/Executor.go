package worker

import (
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"os/exec"
	"time"
)

type Executor struct{

}

var (
	G_executor *Executor
)

//执行任务
func (executor *Executor)ExecuteJob(info *Common.JobExecuteInfo){
	go func() {

		var(
			cmd *exec.Cmd
			err error
			output []byte
			result *Common.JobExecuteResult
			jobLock *JobLock
		)


		result = &Common.JobExecuteResult{
			ExecuteInfo:info,
			Output:make([]byte,0),
		}
		//初始化锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err!=nil{
			result.Err = err
			result.EndTime = time.Now()
		}else{
			result.StartTime = time.Now()
			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			//执行并捕获输出
			output, err = cmd.CombinedOutput()

			//任务执行完成后，把执行结果返回Scheduler，Scheduler会从executingTable中删除掉执行记录
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}

		G_scheduler.PushJobResult(result)
	}()


}

//初始化执行期
func InitExecutor()(err error){
	G_executor = &Executor{}
	return
}

