package tasks

import (
	"errors"
	"reflect"
)

var (
	ErrTaskNotFunc       = errors.New("Task must be Func")
	ErrTaskReturnNoValue = errors.New("Task must return value")
	ErrLastReturnError   = errors.New("Task last return must be error")
)

func VilidateTask(task interface{}) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	if t.Kind() != reflect.Func {
		return ErrTaskNotFunc
	}

	if t.NumOut() < 1 {
		return ErrTaskReturnNoValue
	}

	lastReturn := t.Out(t.NumOut() - 1)
	errInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturn.Implements(errInterface) {
		return ErrLastReturnError
	}

	return nil
}
