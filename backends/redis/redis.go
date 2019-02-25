package redis

import (
	//"github.com/RichardKnop/machinery/v1/common"
	//"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/common"
	"google.golang.org/genproto/googleapis/cloud/redis/v1"
)

type Backend struct {
	common.Backend
	host     string
	password string
	db       int
	pool     *redis.Pool
}
