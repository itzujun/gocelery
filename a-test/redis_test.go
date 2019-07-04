package a_test

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/itzujun/gocelery/common"
	"github.com/itzujun/gocelery/config"
	"testing"
)

func TestRedis(t *testing.T) {

	cfg := &config.RedisConfig{
		MaxIdle:                10,
		MaxActive:              10,
		IdleTimeout:            10,
		Wait:                   true,
		ReadTimeout:            10,
		ConnectTimeout:         10,
		DelayedTasksPollPeriod: 10,
	}

	rdconn := &common.RedisConnector{}

	pool := rdconn.NewPool("", "127.0.0.1:6379", "", 1, cfg, nil)

	conn, err := pool.Dial()
	defer conn.Close()
	if err != nil {
		fmt.Println("redis连接error")
	}
	fmt.Println("连接OK")

	_, err = conn.Do("SET", "name", "open_china")

	if err != nil {
		fmt.Println("error:", err.Error())
	}

	name, err := redis.String(conn.Do("GET", "name"))
	if err != nil {
		fmt.Println("error:", err.Error())
	}
	fmt.Println("name:", name)



}
