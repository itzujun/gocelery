package tasks

import (
	"reflect"
)

// task result
type TaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

func ReflectTaskResult(tsResults []*TaskResult) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(tsResults))
	for i, taskResult := range tsResults {
		resultValue, err := ReflectValue(taskResult.Type, taskResult)
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	return resultValues, nil
}
