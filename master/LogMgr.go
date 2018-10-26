package master

import (
	"context"
	"github.com/anakin/crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
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
	if client, err = mongo.Connect(context.TODO(), G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.EtcdDialTimeOut)*time.Millisecond)); err != nil {
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
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findopt.Sort(logSort),
		findopt.Skip(int64(skip)), findopt.Limit(int64(limit))); err != nil {
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
