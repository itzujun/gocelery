package iface

import (

	"github.com/itzujun/GoCelery/config"
	"github.com/itzujun/GoCelery/tasks"
)

type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}

type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
}
