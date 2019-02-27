package eager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/itzujun/GoCelery/brokers/iface"
	"github.com/itzujun/GoCelery/common"
	"github.com/itzujun/GoCelery/tasks"
)

type Broker struct {
	worker iface.TaskProcessor
	common.Broker
}



func New() iface.Broker {
	return new(Broker)
}



// Mode interface with methods specific for this broker
type Mode interface {
	AssignWorker(p iface.TaskProcessor)
}

func (eagerBroker *Broker) StartConsuming(consumerTag string, concurrency int, p iface.TaskProcessor) (bool, error) {
	return true, nil
}

func (eagerBroker *Broker) StopConsuming() {
	// do nothing
}

func (eagerBroker *Broker) Publish(task *tasks.Signature) error {
	if eagerBroker.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}
	message, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return fmt.Errorf("JSON unmarshal error: %s", err)
	}

	return eagerBroker.worker.Process(signature)
}

func (eagerBroker *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return []*tasks.Signature{}, errors.New("Not implemented")
}

func (eagerBroker *Broker) AssignWorker(w iface.TaskProcessor) {
	eagerBroker.worker = w
}
