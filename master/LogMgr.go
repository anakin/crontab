package master

import (
	"anakin-crontab/common"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)
	t:=time.Duration(G_config.EtcdDialTimeOut) *time.Millisecond
	otps:=options.ClientOptions{
		ConnectTimeout: &t,
	}
	otps.ApplyURI(G_config.MongodbUri)
	if client, err = mongo.Connect(context.TODO(), &otps,
		); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

func (logMgr *LogMgr) ListLog(jobName string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.LogFilter
		logSort *common.SortLogByStartTime
		cursor  mongo.Cursor
		jobLog  *common.JobLog
	)
	logArr = make([]*common.JobLog, 0)
	filter = &common.LogFilter{
		JobName: jobName,
	}
	logSort = &common.SortLogByStartTime{SortOrder: -1}
	opt:=options.Find()
	opt.SetSort(logSort)
	opt.SetLimit(int64(limit))
	opt.SetSkip(int64(skip))
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, opt); err != nil {
		return
	}
	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr, jobLog)
	}
	defer cursor.Close(context.TODO())

	return

}
