package common

import "errors"

var (
	ERR_LOCK_ALREADY_REQUIED = errors.New("lock is busy")
)
