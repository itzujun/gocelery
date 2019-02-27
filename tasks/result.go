package tasks

import (
	"fmt"
	"reflect"
	"strings"
)

type TaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

func ReflectTaskResults(tsResults []*TaskResult) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(tsResults))
	for i, taskResult := range tsResults {
		resultValue, err := ReflectValue(taskResult.Type, taskResult.Value)
		//fmt.Println("lz", "ReflectTaskResults error...")
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	fmt.Println("lz", "ReflectTaskResults ok")
	return resultValues, nil
}

func HumanReadableResults(results []reflect.Value) string {
	if len(results) == 1 {
		return fmt.Sprintf("%v", results[0].Interface())
	}
	readableResults := make([]string, len(results))
	for i := 0; i < len(results); i++ {
		readableResults[i] = fmt.Sprintf("%v", results[i].Interface())
	}
	return fmt.Sprintf("[%s]", strings.Join(readableResults, ", "))
}
