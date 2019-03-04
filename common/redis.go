package common

import (
	"crypto/tls"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/itzujun/GoCelery/config"
	"time"
)

var (
	defaultConfig = &config.RedisConfig{
		MaxIdle:                3,
		IdleTimeout:            240,
		ReadTimeout:            15,
		WriteTimeout:           15,
		ConnectTimeout:         15,
		DelayedTasksPollPeriod: 20,
	}
)

type RedisConnector struct{}

func (rc *RedisConnector) NewPool(socketPath, host, password string, db int, cnf *config.RedisConfig, tlsConfig *tls.Config) *redis.Pool {
	if cnf == nil {
		cnf = defaultConfig
	}
	fmt.Println("MaxIdle:", cnf.MaxActive, )
	fmt.Println("MaxActive:", cnf.MaxActive)
	fmt.Println("Wait:", cnf.Wait)
	return &redis.Pool{
		//MaxIdle:     cnf.MaxActive,
		//IdleTimeout: time.Duration(cnf.IdleTimeout) * time.Second,
		//MaxActive:   cnf.MaxActive,
		MaxIdle:         30,
		IdleTimeout:     240 * time.Second,
		MaxConnLifetime: 240 * time.Second,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			c, err := rc.open(socketPath, host, password, db, cnf, tlsConfig)
			if err != nil {
				return nil, err
			}
			if db != 0 {
				_, err = c.Do("SELECT", db)
				if err != nil {
					return nil, err
				}
			}
			return c, err
		},
	}
}

func (rc *RedisConnector) open(socketPath, host, password string, db int, cnf *config.RedisConfig, tlsConfig *tls.Config) (redis.Conn, error) {
	var opts = []redis.DialOption{
		redis.DialDatabase(db),
		redis.DialConnectTimeout(time.Duration(cnf.ReadTimeout) * time.Second),
		redis.DialWriteTimeout(time.Duration(cnf.WriteTimeout) * time.Second),
		redis.DialConnectTimeout(time.Duration(cnf.ConnectTimeout) * time.Second),
	}

	if tlsConfig != nil {
		opts = append(opts, redis.DialTLSConfig(tlsConfig), redis.DialUseTLS(true))
	}
	if password != "" {
		opts = append(opts, redis.DialPassword(password))
	}
	if socketPath != "" {
		return redis.Dial("unix", socketPath, opts...)
	}

	return redis.Dial("tcp", host, opts...)

}
