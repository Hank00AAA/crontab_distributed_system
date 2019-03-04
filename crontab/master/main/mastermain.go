package main

import (
	"flag"
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/master"
	"runtime"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/master/main/Config"
	"time"
)

func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var(
	confFile string
)

func initArgs(){
	flag.StringVar(&confFile, "config", "./master.json", "制定master.json")
	flag.Parse()
}

func main(){

	var(
		err error
	)

	initArgs()

	//初始化线程
	initEnv()

	if err = Config.InitConfig(confFile);err!=nil{
		goto ERR
	}

	//初始化集群管理器
	if err = master.InitWorkerMgr();err!=nil{
		goto ERR
	}

	//日志管理器
	if err = master.InitLogMgr();err!=nil{
		goto ERR
	}

	//任务管理器
	if err = Config.InitjobMgr();err!=nil{
		goto ERR
	}


	//启动api http 服务
	if err = master.InitApiServer();err!=nil{
		goto ERR
	}

	for{
		time.Sleep(1*time.Second)
	}

	return

	ERR:
		fmt.Println(err)

}