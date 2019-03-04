package main

import (
	"flag"
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/worker"
	"runtime"
	"time"
)


func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var(
	confFile string
)

func initArgs(){
	flag.StringVar(&confFile, "config", "./worker.json", "制定worker.json")
	flag.Parse()
}

func main(){

	var(
		err error
	)

	initArgs()

	//初始化线程
	initEnv()

	if err = worker.InitConfig(confFile);err!=nil{
		goto ERR
	}

	//服务注册
	if err = worker.InitRegister();err!=nil{
		goto ERR
	}

	//启动日志协程
	if err = worker.InitLogSink();err!=nil{
		goto ERR
	}

	//启动执行器
	if err = worker.InitExecutor();err!=nil{
		goto ERR
	}



 	worker.InitScheduler()


	if err = worker.InitjobMgr();err!=nil{
		goto ERR
	}



	for{
		time.Sleep(1*time.Second)
	}

	return

ERR:
	fmt.Println(err)

}
