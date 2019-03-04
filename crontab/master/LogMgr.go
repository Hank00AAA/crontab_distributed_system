package master

import (
	"context"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/master/main/Config"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

//mongodb日志管理
type LogMgr struct{
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr()(err error){
	var(
		client *mongo.Client
	)

	//建立mongodb链接
	if client, err = mongo.Connect(context.TODO(), Config.G_config.MongodbUri);err!=nil{
		return
	}

	//构造单例
	G_logMgr = &LogMgr{
		client:client,
		logCollection:client.Database("cron").Collection("log"),
	}

	return
}

//查看任务日志
func (logMgr *LogMgr)ListLog(name string, skip *int64, limit *int64)(logArr []*Common.JobLog, err error){
	var(
		filter *Common.JobLogFilter
		logSort *Common.SortLogByStarTime
		cursor mongo.Cursor
		jobLog *Common.JobLog
	)

	//len(logArr)
	logArr = make([]*Common.JobLog, 0)

	//过滤条件
	filter = &Common.JobLogFilter{JobName:name}

	//按照任务开始时间安排
	logSort = &Common.SortLogByStarTime{SortOrder:-1}

	if cursor, err = logMgr.logCollection.Find(context.TODO(),filter, &options.FindOptions{Limit:(limit), Skip:(skip),Sort:logSort});err!=nil{
		return
	}
	//延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()){
		jobLog = &Common.JobLog{}

		//反序列化bson
		if err = cursor.Decode(jobLog);err!=nil{
			continue //日志不合法
		}

		logArr = append(logArr, jobLog)
	}

	return
}
