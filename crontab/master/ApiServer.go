package master

import (
	"encoding/json"
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/master/main/Config"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct{
	httpServer *http.Server
}

var(
	G_apiServer *ApiServer
)

//保存任务接口
//POST job={
// "name": "job1",
// "command": "echo hello",
// "cronExpr": "*****"
// }
func handleJobSave( resp http.ResponseWriter, req *http.Request){
	fmt.Println("get post")
	//1.解析POST表单
	var(
		err error
		postJob string
		job Common.Job
		oldJob *Common.Job
		bytes []byte
	)

	if err = req.ParseForm();err!=nil{
		goto ERR
	}

	//2. 取表单job字段
	postJob = req.PostForm.Get("job")

	//3. 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job);err!=nil{
		fmt.Println("1")
		fmt.Println(err)
		goto ERR
	}

	if oldJob, err = Config.G_jobMgr.SaveJob(&job);err!=nil{
		goto ERR
	}

	if bytes, err = Common.BuildResponse(0,"success", oldJob);err==nil{
		resp.Write(bytes)
	}

	return

	ERR:
		if bytes, err = Common.BuildResponse(-1,err.Error(), nil);err==nil{
			resp.Write(bytes)
		}

}

//删除
//POST /job/delete
func handleJobDelete(resp http.ResponseWriter, req *http.Request){
	var(
		err error
		name string
		oldJob *Common.Job
		bytes []byte
	)

	//POST: 解析表单
	if err = req.ParseForm();err!=nil{
		goto ERR
	}

	//删除的任务名
	name = req.PostForm.Get("name")

	//删除任务
	if oldJob,err = Config.G_jobMgr.DeleteJob(name);err!=nil{
		goto ERR
	}

	fmt.Println(oldJob)

	//正常应答
	 if bytes, err = Common.BuildResponse(0,"success", &oldJob);err==nil{
	 	resp.Write(bytes)
	 }

	return

	ERR:
		if bytes, err = Common.BuildResponse(-1, err.Error(), nil);err == nil{
			resp.Write(bytes)
		}
}

func handleJobList(resp http.ResponseWriter, req *http.Request){

	var(
		jobList []*Common.Job
		err error
		bytes []byte
	)

	if jobList, err = Config.G_jobMgr.ListJobs();err!=nil{
		goto ERR
	}

	if bytes, err = Common.BuildResponse(0, "success", jobList);err == nil{
		resp.Write(bytes)
	}


	return
	ERR:
		if bytes, err = Common.BuildResponse(-1, err.Error(), nil);err == nil{
			resp.Write(bytes)
		}
}

//杀死某个任务
//POST /job/kill name
func handleJobKill(resp http.ResponseWriter, req *http.Request){
	var(
		err error
		name string
		bytes []byte
	)

	//解析表单
	if err = req.ParseForm();err!=nil{
		goto ERR
	}

	name = req.PostForm.Get("name")

	//杀死任务
	if err = Config.G_jobMgr.KillJob(name);err!=nil{
		goto ERR
	}

	//正常应答
	if bytes, err = Common.BuildResponse(0, "success", nil);err==nil{
		resp.Write(bytes)
	}
	return

	ERR:
		//异常应答
		if bytes, err = Common.BuildResponse(-1, err.Error(), nil);err == nil{
			resp.Write(bytes)
		}
}

//查询任务日志
func handleJobLog(resp http.ResponseWriter, req *http.Request){

	var(
		err error
		name string //任务名字　
		skipParam string //从第几条开始
		limitParam string //返回多少条
		skip int
		limit int
		logArr []*Common.JobLog
		skip64 int64
		limit64 int64
		bytes []byte
	)

	//解析GET参数
	if err = req.ParseForm();err!=nil{
		goto ERR
	}

	//获取请求参数 /job/log?name=job10&skip=0&limit=9
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")

	if skip, err = strconv.Atoi(skipParam);err!=nil{
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam);err!=nil{
		limit = 20
	}

	skip64 = int64(skip)
	limit64 = int64(limit)

	if logArr, err = G_logMgr.ListLog(name, &skip64, &limit64);err!=nil{
		goto ERR
	}

	if bytes, err = Common.BuildResponse(0, "success", logArr);err==nil{
		resp.Write(bytes)
	}

	return
	ERR:
		if bytes, err = Common.BuildResponse(-1, err.Error(),nil);err==nil{
			resp.Write(bytes)
		}
}

//获取健康worker节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request){
	var(
		workerArr []string
		err error
		bytes []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers();err!=nil{
		goto ERR
	}

	if bytes, err = Common.BuildResponse(0, "Success", workerArr);err==nil{
		resp.Write(bytes)
	}
	return

	ERR:
		if bytes,err = Common.BuildResponse(-1, err.Error(),nil);err==nil{
			resp.Write(bytes)
		}

}

//初始化服务
func InitApiServer()(err error){
	//配置路由
	var(
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir  //静态文件根目录
		staticHandler http.Handler  //静态文件的HTTP回调
	)

	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log",handleJobLog)
	mux.HandleFunc("/worker/list",handleWorkerList)

	//静态文件目录
	staticDir = http.Dir(Config.G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))


	//启动tcp监听
	if listener, err = net.Listen("tcp",":"+strconv.Itoa(Config.G_config.ApiPort));err!=nil{
		return
	}
	//创建http服务
	httpServer = &http.Server{
		ReadTimeout: time.Duration(Config.G_config.ApiReadTimeout)*time.Millisecond,
		WriteTimeout: time.Duration(Config.G_config.ApiWriteTimeout)*time.Millisecond,
		Handler:mux,
	}

	G_apiServer = &ApiServer{
		httpServer:httpServer,
	}

	//启动了服务端
	go httpServer.Serve(listener)


	return

}