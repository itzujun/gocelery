package config

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultResultsExpireIn = 24 * 3600
)

type Config struct {
	Broker          string           `yaml:"broker" envconfig:"BROKER"`
	DefaultQueue    string           `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend   string           `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn int              `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	AMQP            *AMQPConfig      `yaml:"amqp"`
	Redis           *RedisConfig     `yaml:"redis"`
	GCPPubSub       *GCPPubSubConfig `yaml:"-" ignored:"true"`
	MongoDB         *MongoDBConfig   `yamk:"-" ignored:"ture"`
	TLSConfig       *tls.Config
	NoUnixSignals   bool            `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
	DynamoDB        *DynamoDBConfig `yaml:"dynamodb"`
}

type SQSConfig struct {
	WaitTimeSeconds   int  `yaml:"receive_wait_time_seconds" envconfig:"SQS_WAIT_TIME_SECONDS"`
	VisibilityTimeout *int `yaml:"receive_visibility_timeout" envconfig:"SQS_VISIBILITY_TIMEOUT"`
}
type GCPPubSubConfig struct {
	MaxExtension time.Duration
}
type MongoDBConfig struct {
	Database string
}

type DynamoDBConfig struct {
	TaskStatesTable string `yaml:"task_states_table" envconfig:"TASK_STATES_TABLE"`
	GroupMetasTable string `yaml:"group_metas_table" envconfig:"GROUP_METAS_TABLE"`
}

type RedisConfig struct {
	MaxIdle     int `yaml:"max_idle" envconfig:"REDIS_MAX_IDLE"`
	MaxActive   int `yaml:"max_active" envconfig:"REDIS_MAX_ACTIVE"`
	IdleTimeout int `yaml:"max_idle_timeout" envconfig:"REDIS_IDLE_TIMEOUT"`

	Wait           bool `yaml:"wait" envconfig:"REDIS_WAIT"`
	ReadTimeout    int  `yaml:"read_timeout" envconfig:"REDIS_READ_TIMEOUT"`
	WriteTimeout   int  `yaml:"write_timeout" envconfig:"REDIS_WRITE_TIMEOUT"`
	ConnectTimeout int  `yaml:"connect_timeout" envconfig:"REDIS_CONNECT_TIMEOUT"`

	DelayedTasksPollPeriod int `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
}

type QueueBindingArgs map[string]interface{}
type AMQPConfig struct {
	Exchange         string           `yaml:"exchange" envconfig:"AMQP_EXCHANGE"`
	ExchangeType     string           `yaml:"exchange_type" envconfig:"AMQP_EXCHANGE_TYPE"`
	QueueBindingArgs QueueBindingArgs `yaml:"queue_binding_args" envconfig:"AMQP_QUEUE_BINDING_ARGS"`
	BindingKey       string           `yaml:"binding_key" envconfig:"AMQP_BINDING_KEY"`
	PrefetchCount    int              `yaml:"prefetch_count" envconfig:"AMQP_PREFETCH_COUNT"`
}

func (args *QueueBindingArgs) Decode(value string) error {
	pairs := strings.Split(value, ",")
	mp := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		kvpair := strings.Split(pair, ":")
		if len(kvpair) != 2 {
			return fmt.Errorf("invalid map item: %q", pair)
		}
		mp[kvpair[0]] = kvpair[1]
	}
	*args = QueueBindingArgs(mp)
	return nil
}
