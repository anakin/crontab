package worker

import (
	"context"
	"github.com/anakin/crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
)

//mongodb

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink = &LogSink{}
)

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}
func (logSink *LogSink) writeLoop() {

	var (
		jobLog       *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)
	for {
		select {
		case jobLog = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(
					time.Duration(G_config.EtcdDialTimeOut)*time.Millisecond,
					func(logBatch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(logBatch),
				)
			}
			logBatch.Logs = append(logBatch.Logs, jobLog)
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				logBatch = nil
				commitTimer.Stop()
			}

		case timeoutBatch = <-logSink.autoCommitChan:
			if timeoutBatch != logBatch {
				continue
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)
	if client, err = mongo.Connect(context.TODO(), G_config.MongodbUri, clientopt.
		ConnectTimeout(time.Duration(G_config.EtcdDialTimeOut)*time.Millisecond)); err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	go G_logSink.writeLoop()
	return
}

func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default: //dropped,if chan is full

	}
}
