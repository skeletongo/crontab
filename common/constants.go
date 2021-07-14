package common

const (
	// 任务保存目录
	JOB_SAVE_DIR = "/cron/jobs/"

	// 任务强杀目录
	JOB_KILLER_DIR = "/cron/killer/"

	// 任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"

	// 保存任务事件
	JOB_EVENT_SAVE = 1

	// 删除任务事件
	JOB_EVENT_DELETE = 2

	// 强杀任务事件
	JOB_EVENT_KILL = 3

	// 执行太长时间自动强杀
	JOB_EVENT_TIMEOUT_KILL = 4
)

// 内部错误码
const (
	Success     = 0 // 成功
	TimeoutKill = 1 // 执行超时自动强杀
	Kill        = 2 // 主动强杀
)
