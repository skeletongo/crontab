package worker

import (
	"fmt"
	"math"
	"time"

	"github.com/skeletongo/datastructure/queue"

	"github.com/skeletongo/crontab/common"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent             //  etcd任务事件队列
	jobPlanTable      map[string]*queue.Item            // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo // 任务执行表
	jobResultChan     chan *common.JobExecuteResult     // 任务结果队列
	jobQueue          *queue.PriorityQueue
}

var (
	G_scheduler *Scheduler
)

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*queue.Item),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	G_scheduler.jobQueue = queue.NewPriorityQueue(func(a, b interface{}) int {
		t1 := a.(*common.JobSchedulePlan).NextTime
		t2 := b.(*common.JobSchedulePlan).NextTime
		if t1.UnixNano() == t2.UnixNano() {
			return 0
		}
		if t1.UnixNano() < t2.UnixNano() {
			return -1
		}
		return 1
	})

	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(math.MaxInt64)

	// 定时任务common.Job
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			// 对内存中维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)

		case jobResult = <-scheduler.jobResultChan: // 监听任务执行结果
			scheduler.handleJobResult(jobResult)

		case <-scheduleTimer.C: // 最近的任务到期了
			now := time.Now()
			for {
				item := queue.HeapPeek(scheduler.jobQueue)
				jobPlan := item.Value.(*common.JobSchedulePlan)
				if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
					scheduler.TryStartJob(jobPlan)
					jobPlan.NextTime = jobPlan.Expr.Next(now)
					queue.HeapUpdate(scheduler.jobQueue, item)
				} else {
					scheduleTimer.Reset(jobPlan.NextTime.Sub(now))
					break
				}
			}
			continue
		}

		if len(scheduler.jobPlanTable) == 0 {
			scheduleTimer.Reset(math.MaxInt64)
			continue
		}

		jobPlan := queue.HeapPeek(scheduler.jobQueue).Value.(*common.JobSchedulePlan)
		scheduleTimer.Reset(jobPlan.NextTime.Sub(time.Now()))
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		jobSchedulePlan, err := common.BuildJobSchedulePlan(jobEvent.Job)
		if err != nil {
			return
		}
		if item, ok := scheduler.jobPlanTable[jobEvent.Job.Name]; ok {
			item.Value = jobSchedulePlan
			queue.HeapUpdate(scheduler.jobQueue, item)
		} else {
			item := &queue.Item{Value: jobSchedulePlan}
			scheduler.jobPlanTable[jobEvent.Job.Name] = item
			queue.HeapPush(scheduler.jobQueue, item)
		}

	case common.JOB_EVENT_DELETE: // 删除任务事件
		if item, jobExisted := scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
			queue.HeapRemove(scheduler.jobQueue, item.GetIndex())
		}

	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消掉Command执行, 判断任务是否在执行中
		if jobExecuteInfo, jobExecuting := scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.Errcode = common.Kill
			jobExecuteInfo.CancelFunc() // 触发command杀死shell子进程, 任务得到退出
		}

	case common.JOB_EVENT_TIMEOUT_KILL: // 执行超时自动强杀
		if jobExecuteInfo, jobExecuting := scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.Errcode = common.TimeoutKill
			jobExecuteInfo.CancelFunc()

		}
	}
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
			Errcode:      result.ExecuteInfo.Errcode,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		GlobalLogSink.Append(jobLog)
	}

	// fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度 和 执行 是2件事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		// fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime, jobPlan.Job)
	G_executor.ExecuteJob(jobExecuteInfo)
}
