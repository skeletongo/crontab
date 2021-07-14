package worker

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/skeletongo/crontab/common"
)

// mongodb存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	// 单例
	GlobalLogSink *LogSink
)

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	ticker := time.NewTicker(time.Second)
	logBatch := new(common.LogBatch)

	for {
		select {
		case <-ticker.C:
			if len(logBatch.Logs) > 0 {
				logSink.saveLogs(logBatch)
				logBatch.Logs = logBatch.Logs[:0]
			}

		case jobLog := <-logSink.logChan:
			logBatch.Logs = append(logBatch.Logs, jobLog)
			if len(logBatch.Logs) >= GlobalConfig.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				logBatch.Logs = logBatch.Logs[:0]
			}
		}
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	// 建立mongodb连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(GlobalConfig.MongodbConnectTimeout)*time.Millisecond)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(GlobalConfig.MongodbUri)); err != nil {
		return
	}

	//   选择db和collection
	GlobalLogSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go GlobalLogSink.writeLoop()
	log.Println("LogSink success")
	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
