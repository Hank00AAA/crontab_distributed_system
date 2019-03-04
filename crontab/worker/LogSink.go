package worker

import (
	"context"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"time"
)

//mongoDB存储日志
type LogSink struct{
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *Common.JobLog
	autoCommitCHan chan *Common.LogBatch
}

var(
	//单例
	G_logSink *LogSink
)

func (logSink *LogSink)saveLogs(batch *Common.LogBatch){
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

//日志存储协程
func(logSink *LogSink)writeLoop(){
	var(
		log *Common.JobLog
		logBatch *Common.LogBatch //当前批次
		commitTimer *time.Timer
		timeoutBatch *Common.LogBatch //超时批次
	)

	for{
		select {
		case log = <- logSink.logChan:
			// 把日志写到mongodb中
			//logSink.logCollection.InsertOne
			//因为每次插入需要等待mongodb的一次请求往返，耗时可能因为网络慢耗时多
			//所以尽量使用批次插入
			//给1秒时间超时自动提交

			if logBatch == nil{
				logBatch = &Common.LogBatch{}
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *Common.LogBatch) func(){
						return func(){
							logSink.autoCommitCHan <- batch
						}
					}(logBatch),
				)

			}

			//追加新日志
			logBatch.Logs = append(logBatch.Logs, log)

			//如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize{
				//发送日志
				logSink.saveLogs(logBatch)

				//清空logbatch
				logBatch = nil

				//取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitCHan: //过期批次
			//判断过期批次是否仍旧为当前batch
			if timeoutBatch != logBatch{
				continue
			}
			//把批次写入到mongodb中去
			logSink.saveLogs(timeoutBatch)
			//清空logbatch
			logBatch = nil
		}
	}
}


//发送日志
func (logSink *LogSink) Append(jobLog *Common.JobLog){

	select{
	case logSink.logChan <- jobLog:
	default:
		//队列满丢弃
	}
}

func InitLogSink() (err error){

	var(
		client *mongo.Client
	)

	if client, err = mongo.Connect(context.TODO(), G_config.MongodbUri);err!=nil{
		return
	}

	//选择db和collection
	G_logSink = &LogSink{
		client:client,
		logCollection:client.Database("cron").Collection("log"),
		logChan:make(chan *Common.JobLog, 1000),
		autoCommitCHan:make(chan *Common.LogBatch, 1000),
	}

	//启动一个mongodb处理协程
	go G_logSink.writeLoop()

	return
}