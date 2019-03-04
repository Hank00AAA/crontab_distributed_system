package worker

import (
	"context"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"go.etcd.io/etcd/clientv3"
)

//分布式锁
type JobLock struct{
	//etcd 客户端
	kv clientv3.KV
	lease clientv3.Lease
	jobName string //锁哪个任务
	cancelFunc context.CancelFunc //用于终止自动续租
	leaseId clientv3.LeaseID //租约id
	isLocked bool
}

//初始化锁
func InitJobLock(jobName string , kv clientv3.KV, lease clientv3.Lease)(jobLock *JobLock){
	jobLock = &JobLock{
		kv:kv,
		lease:lease,
		jobName:jobName,
	}
	return
}

//上锁函数
func(jobLock *JobLock)TryLock() (err error){

	var(
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)

	//1.创建租约
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5);err!=nil{
		return
	}

	//2. 自动续租

	//创建context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	//租约id
	leaseId = leaseGrantResp.ID
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId);err!=nil{
		goto FAIL
	}

	//3.处理续租应答的协程
	go func() {

		var(
			keepResp *clientv3.LeaseKeepAliveResponse
		)

		for{
			select{
			case keepResp = <- keepRespChan: //自动续租应答
				if keepResp==nil{
					goto END
				}
			}
		}

		END:

	}()


	//4. 创建事务txn
	txn = jobLock.kv.Txn(context.TODO())

	//锁路径
	lockKey = Common.JOB_LOCK_DIR + jobLock.jobName

	//5. 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	//提交事务
	if txnResp, err = txn.Commit();err!=nil{
		goto FAIL
	}

	//6. 成功则返回，失败回滚，释放租约
	if !txnResp.Succeeded{ //锁被占用，if条件不成立
		err = Common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

FAIL:
	cancelFunc() //取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId) //释放租约
	return
}

func (jobLock *JobLock)Unlock(){
	if jobLock.isLocked {
		jobLock.cancelFunc()                                  //取消程序自动续租
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) //释放租约
	}
}