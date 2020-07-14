package result

import (
	"errors"
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/itzujun/gocelery/backends/iface"
	"github.com/itzujun/gocelery/tasks"
)

var (
	ErrBackendNotConfigured = errors.New("result backend not configured")
	ErrTimeoutReached       = errors.New("timeout reached")
)

// async result
type AsyncResult struct {
	Signature *tasks.Signature
	taskState *tasks.TaskState
	backend   iface.Backend
}

type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           iface.Backend
}

type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      iface.Backend
}

func NewAsyncResult(signature *tasks.Signature, backend iface.Backend) *AsyncResult {

	return &AsyncResult{
		Signature: signature,
		taskState: new(tasks.TaskState),
		backend:   backend,
	}
}

func NewChordAsyncResult(groupTasks []*tasks.Signature, chordCallback *tasks.Signature, backend iface.Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}

	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}

}

func NewChainAsyncResult(tasks []*tasks.Signature, backend iface.Backend) *ChainAsyncResult {
	asyncResult := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResult[i] = NewAsyncResult(task, backend)
	}

	return &ChainAsyncResult{
		asyncResults: asyncResult,
		backend:      backend,
	}
}

func (asyncResult *AsyncResult) GetState() *tasks.TaskState {
	// todo think
	if asyncResult.taskState.IsCompleted() { // have comtaleted
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(asyncResult.Signature.UUID)
	if err == nil {
		asyncResult.taskState = taskState
	}
	return asyncResult.taskState
}

func (asyncResult *AsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	for {
		results, err := asyncResult.Touch()

		if results == nil && err == nil {
			time.Sleep(sleepDuration)
		} else {
			return results, err
		}
	}
}

func (asyncResult *AsyncResult) Touch() ([]reflect.Value, error) {

	if asyncResult.backend == nil {
		return nil, result.ErrBackendNotConfigured
	}

	asyncResult.GetState()

	if asyncResult.backend.IsAMQP() && asyncResult.taskState.IsCompleted() {
		_ = asyncResult.backend.PurgeState(asyncResult.taskState.TaskUUID)
	}

	if asyncResult.taskState.IsFailure() { // failure
		return nil, errors.New(asyncResult.taskState.Error)
	}

	if asyncResult.taskState.IsSuccess() { // success
		return tasks.ReflectTaskResults(asyncResult.taskState.Results)
	}

	return nil, nil
}
