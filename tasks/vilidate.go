package tasks

import (
	"errors"
	"reflect"
)

var (
	ErrTaskNotFunc       = errors.New("task must be func")
	ErrTaskReturnNoValue = errors.New("task must return value")
	ErrLastReturnError   = errors.New("task last return must be error")
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
