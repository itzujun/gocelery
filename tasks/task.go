package tasks

import (
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"reflect"
)

var ErrTaskPanicked = errors.New("Invoking task caused a panic")

type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Context    context.Context
	Args       []reflect.Value
}

// build new Task
func New(taskFunc interface{}, args [] Arg) (*Task, error) {
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
	}
	//todo

	// Arg check
	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}
	return task, nil
}

// func remote call
func (task *Task) Call() ([]*TaskResult, error) {

	if span := opentracing.SpanFromContext(task.Context); span != nil {
		defer span.Finish()
	}

	defer func() { //日志输出打印

	}()

	args := task.Args
	//if task.UseContext {
	//	ctxValue := reflect.ValueOf(task.Context)
	//	args = append([]reflect.Value{ctxValue}, args...)
	//}

	// do the task

	if len(args) > 0 {
		fmt.Println("value:", args[0].Interface())
	}

	//args=[]reflect.Value{reflect.ValueOf("世界你好")}
	//fmt.Println("args:", args)

	//results := task.TaskFunc.Call(args)
	results := task.TaskFunc.Call(args)
	fmt.Println("GoClery执行结果", results)

	if len(results) == 0 {
		return nil, ErrTaskReturnNoValue
	}

	lastResult := results[len(results)-1]

	if !lastResult.IsNil() {
		retryErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retryErrorInterface) {
			return nil, lastResult.Interface().(ErrRetryTaskLater)
		}

		// 最后一个判定是否是nil
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnError
		}

		return nil, lastResult.Interface().(error)
	}
	taskResults := make([]*TaskResult, len(results)-1) //sub error

	fmt.Println("返回函数长度:", len(results))
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		fmt.Println("设置返回数据:", results[i].Interface())
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
		}
	}
	return taskResults, nil
}

func (task *Task) ReflectArgs(args [] Arg) error {
	argValues := make([]reflect.Value, len(args))
	for i, arg := range args {
		argValue, err := ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return err
		}
		argValues[i] = argValue.Elem()
	}
	task.Args = argValues
	return nil
}
