package common

import (
	"errors"
	"fmt"
	"github.com/itzujun/gocelery/brokers/iface"
	"github.com/itzujun/gocelery/config"
	"github.com/itzujun/gocelery/retry"
	"github.com/itzujun/gocelery/tasks"
)

type Broker struct {
	cnf                 *config.Config
	registeredTaskNames []string
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
}

func NewBroker(cnf *config.Config) Broker {
	return Broker{cnf: cnf, retry: true}
}
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

func (b *Broker) GetRetry() bool {
	return b.retry
}

func (b *Broker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

func (b *Broker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

func (b *Broker) GetStopChan() chan int {
	return b.stopChan
}

func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames = names
}
func (b *Broker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range b.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}
	b.stopChan = make(chan int)
	b.retryStopChan = make(chan int)
}

func (b *Broker) StopConsuming() {
	b.retry = false
	select {
	case b.retryStopChan <- 1:
		fmt.Println("Stopping retry closure.")
	default:
	}

	select {
	case b.stopChan <- 1:
		fmt.Println("Stop channel")
	default:

	}
}

func (b *Broker) GetRegisteredTaskNames() []string {
	return b.registeredTaskNames
}

func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}
	s.RoutingKey = b.GetConfig().DefaultQueue
}
