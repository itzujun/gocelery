package tasks

import (
	"fmt"
	"time"
)

type ErrRetryTaskLater struct {
	name, msg string
	retryIn   time.Duration
}

//func RetryIn(e ErrRetryTaskLater) time.Duration {
//	return e.retryIn
//}

func (e *ErrRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

func NewErrRetryTaskLater(msg string, retryIn time.Duration) ErrRetryTaskLater {
	return ErrRetryTaskLater{msg: msg, retryIn: retryIn}
}

func (e ErrRetryTaskLater) Error() string {
	return fmt.Sprintf("Task error %s will retry in %s", e.msg, e.retryIn)
}

type Retriable interface {
	RetryIn() time.Duration
}
