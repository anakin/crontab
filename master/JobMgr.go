package master

import (
	"anakin-crontab/common"
	"context"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() error {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut) * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return err
	}

	kv = clientv3.KV(client)
	lease = clientv3.Lease(client)
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return nil
}
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {

	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {

	var (
		jobKey     string
		deleteResp *clientv3.DeleteResponse
		oldJobObj  common.Job
	)

	jobKey = common.JOB_SAVE_DIR + name
	if deleteResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(deleteResp.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobMgr) ListJobs() ([]*common.Job, error) {

	// 任务保存的目录
	dirKey := common.JOB_SAVE_DIR

	jobList := make([]*common.Job, 0)
	// 获取目录下所有任务信息
	getResp, err := jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return jobList, err
	}

	// 初始化数组空间
	// len(jobList) == 0

	// 遍历所有任务, 进行反序列化
	for _, kvPair := range getResp.Kvs {
		job := &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return jobList, nil
}

func (jobMgr *JobMgr) KillJob(name string) error {

	jobKey := common.JOB_KILL_DIR + name
	leaseGrantResp, err := jobMgr.lease.Grant(context.TODO(), 1)
	if err != nil {
		return err
	}
	leaseId := leaseGrantResp.ID
	if _, err = jobMgr.kv.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseId)); err != nil {
		return err
	}
	return nil
}
