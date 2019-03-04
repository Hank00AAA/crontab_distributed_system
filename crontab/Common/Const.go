package Common

const(
	//任务保存目录
	JOB_SAVE = "/cron/jobs/"

	//任务强杀目录
	JOB_KILLER_DIR = "/cron/killer"

	//保存任务事件
	JOB_EVENT_SAVE = 1

	//删除任务事件

	JOB_EVENT_DELETE = 2

	//任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	//强杀任务时间
	JOB_EVENT_KILLER = 3

	//服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"

)
