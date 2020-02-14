package worker

import (
	"anakin-crontab/common"
	"context"
	"go.etcd.io/etcd/clientv3"
)

type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string
	cancelFunc context.CancelFunc
	leaseId    clientv3.LeaseID
	isLocked   bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
}

func (jobLock *JobLock) TryLock() error {
	var (
		txn     clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	leaseGrantResp, err := jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}

	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	leaseId := leaseGrantResp.ID
	keepRespChan, err := jobLock.lease.KeepAlive(cancelCtx, leaseId)
	if err != nil {
		goto FAIL
	}
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()
	txn = jobLock.kv.Txn(context.TODO())
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}
	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIED
		goto FAIL
	}

	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return nil
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseId)
	return nil
}

func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
}
