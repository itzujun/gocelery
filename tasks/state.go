package tasks

import (
	"time"
)

const (
	StatePending  = "PENDING"
	StateReceived = "RECEIVED"
	StateStarted  = "STARTED"
	StateRetry    = "RETRY"
	StateSuccess  = "SUCCESS"
	StateFailure  = "FAILURE"
)

type TaskState struct {
	TaskUUID  string        `bson:"_id"`
	TaskName  string        `bson:"task_name"`
	State     string        `bson:"state"`
	Results   []*TaskResult `bson:"results"`
	Error     string        `bson:"error"`
	CreatedAt time.Time     `bson:"created_at"`
}

func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure()
}

func (taskState *TaskState) IsSuccess() bool {
	return taskState.State == StateSuccess
}

func (taskState *TaskState) IsFailure() bool {
	return taskState.State == StateFailure
}
