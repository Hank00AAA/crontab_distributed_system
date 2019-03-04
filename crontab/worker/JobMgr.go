package worker

import (
	"context"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}


var(
	G_jobMgr *JobMgr
)

//监听任务变化
func (jobMgr *JobMgr)watchJobs()(err error){

	var(
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *Common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *Common.JobEvent
	)

	//1. get以下/cron/jobs/目录下所有任务，并且获知当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), Common.JOB_SAVE, clientv3.WithPrefix());err!=nil{
		return
	}


	//遍历 当前有哪些任务
	for _, kvPair = range getResp.Kvs{
		//反序列化json得到job
		if job, err = Common.UnpackJob(kvPair.Value);err==nil{
			jobEvent = Common.BuildJobEvent(Common.JOB_EVENT_SAVE, job)
			//TODO:把job同步这scheduler（调度协程）
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	//2. 从该revision向后监听变化
	go func() {//监听协程
	//从get时刻的后续版本开始监听
		watchStartRevision = getResp.Header.Revision + 1

		//监听后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), Common.JOB_SAVE, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		//处理监听事件
		for watchResp = range watchChan{
			for _, watchEvent = range watchResp.Events{
				switch(watchEvent.Type){
				case mvccpb.PUT: //任务保存
					if job, err = Common.UnpackJob(watchEvent.Kv.Value);err!=nil{
						continue
					}
				//构造一个更新event
				jobEvent = Common.BuildJobEvent(Common.JOB_EVENT_SAVE, job)

				case mvccpb.DELETE: //任务被删除了
				//Delete /cron/jobs/job10
				jobName = Common.ExtractJobName(string(watchEvent.Kv.Key))
				job = &Common.Job{Name:jobName}
				//构造一个删除Event
				jobEvent = Common.BuildJobEvent(Common.JOB_EVENT_DELETE, job)

				}
			}
			//TODO:推给sheduler
			//G_Scheduler.PushJobEvent(jobEvent)
			G_scheduler.PushJobEvent(jobEvent)
			}


	}()

	return
}

func InitjobMgr()(err error){
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)

	config = clientv3.Config{
		Endpoints:G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,
	}

	if client, err = clientv3.New(config);err!=nil{
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
		watcher:watcher,
	}

	//启动job监听
	G_jobMgr.watchJobs()

	//启动监听killer
	G_jobMgr.watchKiller()


	return


}

//监听任务强杀
func (jobMgr *JobMgr)watchKiller(){

	var(
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *Common.JobEvent
		jobName string
		job *Common.Job
	)

	//监听/cron/killer/目录
	go func() {
		watchChan = jobMgr.watcher.Watch(context.TODO(),Common.JOB_KILLER_DIR, clientv3.WithPrefix())

		for watchResp = range watchChan{
			for _, watchEvent = range watchResp.Events{
				switch watchEvent.Type{
				case mvccpb.PUT: //杀死任务
					jobName = Common.ExtractKillerName(string(watchEvent.Kv.Key)) //cron/killer/job10
					job = &Common.Job{Name:jobName}
					jobEvent = Common.BuildJobEvent(Common.JOB_EVENT_KILLER, job)
					//事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //Killer标记过期，被自动删除


				}
			}
		}



	}()



}

//创建任务执行锁
func (jobMgr *JobMgr)CreateJobLock(jobName string)(jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}

