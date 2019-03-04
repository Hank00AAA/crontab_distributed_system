package Config

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}


var(
	G_jobMgr *JobMgr
)

func InitjobMgr()(err error){
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
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

	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
	}

	return


}

func (jobMgr *JobMgr)SaveJob(job *Common.Job)(oldJob *Common.Job, err error){
	//把任务保存到/cron/jobs/任务名 -> json

	var(
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj Common.Job
	)

	jobKey = Common.JOB_SAVE + job.Name
	if jobValue, err = json.Marshal(job);err!=nil{
		return
	}
	//保存到etcd
	if putResp, err = G_jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV());err != nil{
		return
	}

	//如果是更新，返回旧值
	if putResp.PrevKv != nil{
		//对旧值反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj);err!=nil{
			err = nil
			return
		}
	}
	oldJob = &oldJobObj
	return

}

func (jobMgr *JobMgr)DeleteJob(name string)(oldJob *Common.Job, err error){
	var(
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj Common.Job
	)

	//etcd中的name
	jobKey = Common.JOB_SAVE + name

	//从etcd删除
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV());err!=nil{
		return
	}

	if len(delResp.PrevKvs)!=0{
		//解析旧值
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj);err!=nil{
			err = nil
			return
		}
		fmt.Println(oldJobObj)
		oldJob = &oldJobObj
	}

	return
}

//列举任务
func (jobMgr *JobMgr)ListJobs()(jobList []*Common.Job, err error){
	var(
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *Common.Job
	)

	dirKey = Common.JOB_SAVE
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix());err!=nil{
		return
	}

	//初始化数组
	jobList = make([]*Common.Job, 0)

	//遍历所有任务进行反序列化
	for _, kvPair = range getResp.Kvs{
		job = &Common.Job{}
		if err = json.Unmarshal(kvPair.Value, &job);err!=nil{
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}

	return
}

//杀死任务
func (jobmgr *JobMgr)KillJob(name string)(err error){
	//更新key = /cron/killer/任务名
	var(
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	//通知worker杀死任务
	killerKey = Common.JOB_KILLER_DIR + name

	//在etcd更新,让worker监听一次put操作
	//让它自动过期
	if leaseGrantResp,err = jobmgr.lease.Grant(context.TODO(), 1);err!=nil{
		return
	}

	//租约ID
	leaseId = leaseGrantResp.ID

	//设置killer标记
	if _, err = jobmgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId));err!=nil{
		return
	}
	return
}