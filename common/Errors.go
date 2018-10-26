package common

import "errors"

var (
	ERR_LOCK_ALREADY_REQUIED = errors.New("lock is busy")
	ERR_NO_LOCAL_IP_FOUND    = errors.New("no local ip")
)
