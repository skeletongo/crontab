package master

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.etcd.io/etcd/client/v3"

	"github.com/skeletongo/crontab/common"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	GlobalJobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	// 初始化配置
	config := clientv3.Config{
		Endpoints:   GlobalConfig.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(GlobalConfig.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	// 赋值单例
	GlobalJobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	log.Println("JobMgr success")
	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到/cron/jobs/任务名 -> json
	// etcd的保存key
	jobKey := common.JOB_SAVE_DIR + job.Name
	// 任务信息json
	jobValue, err := json.Marshal(job)
	if err != nil {
		return
	}
	// 保存到etcd
	putResp, err := jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return
	}
	// 如果是更新, 那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		oldJobObj := common.Job{}
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	// etcd中保存任务的key
	jobKey := common.JOB_SAVE_DIR + name

	// 从etcd中删除它
	delResp, err := jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		// 解析一下旧值, 返回它
		oldJobObj := common.Job{}
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 列举任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	// 任务保存的目录
	dirKey := common.JOB_SAVE_DIR

	// 获取目录下所有任务信息
	getResp, err := jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return
	}

	// 遍历所有任务, 进行反序列化
	for _, kvPair := range getResp.Kvs {
		job := &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名

	// 通知worker杀死对应任务
	killerKey := common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	leaseGrantResp, err := jobMgr.lease.Grant(context.TODO(), 1)
	if err != nil {
		return
	}

	// 租约ID
	leaseId := leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}
