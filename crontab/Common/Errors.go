package Common

import "github.com/pkg/errors"

var(
	ERR_LOCK_ALREADY_REQUIRED = errors.New("锁已被占用")

	ERR_NO_LOCAL_IP_FOUND = errors.New("没有网卡ip")
)