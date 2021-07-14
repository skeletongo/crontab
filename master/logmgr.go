package master

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/skeletongo/crontab/common"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	GlobalLogMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(GlobalConfig.MongodbConnectTimeout)*time.Millisecond)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(GlobalConfig.MongodbUri)); err != nil {
		return
	}

	GlobalLogMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	log.Println("LogMgr success")
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)

	// len(logArr)
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 查询
	op := options.Find().SetSort(logSort).SetSkip(int64(skip)).SetLimit(int64(limit))
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, op); err != nil {
		return
	}
	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}
	return
}
