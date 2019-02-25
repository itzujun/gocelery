package tasks

import (
	"context"
	"reflect"
)

type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Context    context.Context
	Args       []reflect.Value
}
