package gocelery

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	backendsiface "github.com/itzujun/gocelery/backends/iface"
	"github.com/itzujun/gocelery/backends/result"
	"github.com/itzujun/gocelery/brokers/eager"
	brokersiface "github.com/itzujun/gocelery/brokers/iface"
	"github.com/itzujun/gocelery/config"
	"github.com/itzujun/gocelery/tasks"
	"sync"
)

type Server struct {
	config            *config.Config
	registeredTasks   map[string]interface{}
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	prePublishHandler func(signature *tasks.Signature)
}

func NewServerWithBrokerBackend(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend) *Server {
	return &Server{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		broker:          brokerServer,
		backend:         backendServer,
	}
}

func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}
	// Backend is optional so we ignore the error
	backend, _ := BackendFactory(cnf)
	srv := NewServerWithBrokerBackend(cnf, broker, backend)
	eager, ok := broker.(eager.Mode)
	if ok {
		eager.AssignWorker(srv.NewWorker("eager", 0))
	}
	return srv, nil
}

func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

func (server *Server) GetBackend() backendsiface.Backend {
	return server.backend
}

func (server *Server) SetBackend(backend backendsiface.Backend) {
	server.backend = backend
}

func (server *Server) GetConfig() *config.Config {
	return server.config
}

func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
}

func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = handler
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

func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks[name]
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

//获取注册信息 func name
func (server *Server) GetRegisteredTaskNames() []string {
	tasksNames := make([]string, len(server.registeredTasks))
	var i = 0
	for name := range server.registeredTasks {
		tasksNames[i] = name
		i++
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

func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {

	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorChan := make(chan error, len(group.Tasks)*2)

	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

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
