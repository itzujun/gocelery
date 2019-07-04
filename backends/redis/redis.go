package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/redsync"
	"github.com/garyburd/redigo/redis"
	"github.com/itzujun/gocelery/backends/iface"
	"github.com/itzujun/gocelery/common"
	"github.com/itzujun/gocelery/config"
	"github.com/itzujun/gocelery/tasks"
	"time"
)

type Backend struct {
	common.Backend
	host       string
	password   string
	db         int
	pool       *redis.Pool
	socketPath string
	redsync    *redsync.Redsync
	common.RedisConnector
}

func New(cnf *config.Config, host, password, socketPath string, db int) iface.Backend {
	return &Backend{
		Backend:    common.NewBackend(cnf),
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	}
}

func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	// 创建并设置到redis数据库中
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return err
	}

	// 设置任务到期时间
	return b.setExpirationTime(groupUUID)
}

func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}
	return countSuccessTasks == groupTaskCount, nil
}

func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}
	return b.getStates(groupMeta.TaskUUIDs...)
}

func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	conn := b.open()
	defer conn.Close()
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}
	if groupMeta.ChordTriggered {
		return false, nil
	}
	groupMeta.ChordTriggered = true
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	conn := b.open()
	defer conn.Close()
	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}
	// 获取交易状态
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}
	return state, nil
}

// Del
func (b *Backend) PurgeState(taskUUID string) error {
	conn := b.open()
	defer conn.Close()
	_, err := conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}
	return nil
}

// gropup Del
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", groupUUID)
	if err != nil {
		return err
	}
	return nil
}

// get group
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return nil, err
	}
	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}
	return groupMeta, nil
}

func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))
	conn := b.open()
	defer conn.Close()

	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		taskUUIDInterfaces[i] = interface{}(taskUUID)
	}

	// 返回一个或者是多个的值
	reply, err := redis.Values(conn.Do("MGET", taskUUIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("Expected byte array, instead got: %v", value)
		}

		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			fmt.Println("error:", err.Error())
			return taskStates, err
		}
		taskStates[i] = taskState
	}
	return taskStates, nil
}

func (b *Backend) updateState(taskState *tasks.TaskState) error {
	conn := b.open()
	defer conn.Close()
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", taskState.TaskUUID, encoded)
	if err != nil {
		return err
	}
	return b.setExpirationTime(taskState.TaskUUID)
}

func (b *Backend) setExpirationTime(key string) error {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		expiresIn = config.DefaultResultsExpireIn
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("EXPIREAT", key, expirationTimestamp)
	if err != nil {
		return err
	}

	return nil
}

// open returns or creates instance of Redis connection
func (b *Backend) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
	}
	// todo
	//if b.redsync == nil {
	//	var pools = []redsync.Pool{b.pool}
	//	b.redsync = redsync.New(pools)
	//}
	return b.pool.Get()
}
