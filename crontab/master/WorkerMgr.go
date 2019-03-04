package master

import (
	"context"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/master/main/Config"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// /cron/workers
type WorkerMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

}

var(
	G_workerMgr *WorkerMgr
)

//获取在线worker列表
func (workerMgr *WorkerMgr)ListWorkers()(workerArr []string, err error){

	var(
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIP string
	)

	//初始化数组
	workerArr = make([]string, 0)

	//获取目录下所有kv
	if getResp, err = workerMgr.kv.Get(context.TODO(), Common.JOB_WORKER_DIR, clientv3.WithPrefix());err!=nil{
		return
	}

	//解析每个节点的IP
	for _, kv = range getResp.Kvs{
		//kv.Key: /cron/workers/192.168.2.1
		workerIP = Common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}

	return
}

func InitWorkerMgr()(err error){

	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	//初始化配置
	config = clientv3.Config{
		Endpoints:Config.G_config.EtcdEndpoints,
		DialTimeout:time.Duration(Config.G_config.EtcdDialTimeout)*time.Millisecond,
	}

	//建立链接
	if client,err = clientv3.New(config);err!=nil{
		return
	}

	//得到KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client:client,
		kv:kv,
		lease:lease,
	}

	return
}
