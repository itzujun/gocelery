package GoCelery

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	backendiface "github.com/itzujun/GoCelery/backends/iface"
	brokeriface "github.com/itzujun/GoCelery/brokers/iface"
	"github.com/itzujun/GoCelery/brokers/result"
	"github.com/itzujun/GoCelery/config"
	"github.com/itzujun/GoCelery/tasks"
	"sync"
)

type Server struct {
	config            *config.Config
	registeredTasks   map[string]interface{}
	broker            brokeriface.Broker
	backend           backendiface.Backend
	prePublishHandler func(signature *tasks.Signature)
}

func (server *Server) RegisterTasks(taskFuncs map[string]interface{}) error {
	for _, task := range taskFuncs {
		if err := tasks.VilidateTask(task); err != nil {
			return err
		}
	}
	server.registeredTasks = taskFuncs
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.VilidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks[name] = taskFunc
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks[name]
	return ok
}

//获取注册信息 func name
func (server *Server) GetRegisteredTaskNames() []string {
	tasksNames := make([]string, len(server.registeredTasks))
	var i = 0
	for name := range server.registeredTasks {
		tasksNames[i] = name
	}
	return tasksNames
}

// 推送执行任务
func (server *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {

	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	if err := server.broker.Publish(signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %s", err)
	}
	return result.NewAsyncResult(signature, server.backend), nil

}

func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {

	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorChan := make(chan error, len(group.Tasks)*2)

	server.backend.InitGroup(group.GroupUUid, group.GetUUIDs())

	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {
		if sendConcurrency > 0 {
			<-pool
		}
		go func(s *tasks.Signature, index int) {
			defer wg.Done()
			err := server.broker.Publish(s)
			if sendConcurrency > 0 {
				pool <- struct{}{}
			}
			if err != nil {
				errorChan <- fmt.Errorf("Publish message error: %s", err)
				return
			}
			asyncResults[index] = result.NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil


	}

}
