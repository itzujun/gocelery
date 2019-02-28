package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type ErrUnsupportedType struct {
	valueType string
}

func NewErrUnsupportedType(valueType string) ErrUnsupportedType {
	return ErrUnsupportedType{valueType}
}

func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("%v is not one of supported types", e.valueType)
}

var (
	typeMap = map[string]reflect.Type{
		"bool":    reflect.TypeOf(true),
		"int":     reflect.TypeOf(int(1)),
		"int8":    reflect.TypeOf(int8(1)),
		"int16":   reflect.TypeOf(int16(1)),
		"int32":   reflect.TypeOf(int32(1)),
		"int64":   reflect.TypeOf(int64(1)),
		"uint":    reflect.TypeOf(uint(1)),
		"uint8":   reflect.TypeOf(uint8(1)),
		"uint16":  reflect.TypeOf(uint16(1)),
		"uint32":  reflect.TypeOf(uint32(1)),
		"uint64":  reflect.TypeOf(uint64(1)),
		"float32": reflect.TypeOf(float32(0.5)),
		"float64": reflect.TypeOf(float64(0.5)),
		"string":  reflect.TypeOf(string("")),
		// slices
		"[]bool":    reflect.TypeOf(make([]bool, 0)),
		"[]int":     reflect.TypeOf(make([]int, 0)),
		"[]int8":    reflect.TypeOf(make([]int8, 0)),
		"[]int16":   reflect.TypeOf(make([]int16, 0)),
		"[]int32":   reflect.TypeOf(make([]int32, 0)),
		"[]int64":   reflect.TypeOf(make([]int64, 0)),
		"[]uint":    reflect.TypeOf(make([]uint, 0)),
		"[]uint8":   reflect.TypeOf(make([]uint8, 0)),
		"[]uint16":  reflect.TypeOf(make([]uint16, 0)),
		"[]uint32":  reflect.TypeOf(make([]uint32, 0)),
		"[]uint64":  reflect.TypeOf(make([]uint64, 0)),
		"[]float32": reflect.TypeOf(make([]float32, 0)),
		"[]float64": reflect.TypeOf(make([]float64, 0)),
		"[]string":  reflect.TypeOf([]string{""}),
	}

	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()

	typeConvError = func(argValue interface{}, argTypeStr string) error {
		return fmt.Errorf("%v is not %v", argValue, argTypeStr)
	}
)

type ErrorUnsupportedType struct {
	valueType string
}

func NewErrorUnsupportedType(value string) ErrorUnsupportedType {
	return ErrorUnsupportedType{valueType: value}
}

func (e ErrorUnsupportedType) Error() string {
	return fmt.Sprint("%v is not one of supported types", e.valueType)
}

func ReflectValue(valueType string, value interface{}) (reflect.Value, error) {
	if strings.HasPrefix(valueType, "[]") {
		return reflectValues(valueType, value)
	}
	return reflectValue(valueType, value)
}

func reflectValue(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typeMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrorUnsupportedType(valueType)
	}
	theValue := reflect.New(theType)

	//booleans
	if theType.String() == "bool" {
		boolValue, err := getBoolValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		//theValue.Elem().SetBool(boolValue)
		theValue = reflect.ValueOf(boolValue)
		return theValue, nil
	}

	// int  int64
	if strings.HasSuffix(theType.String(), "int") || strings.HasSuffix(theType.String(), "int64") {
		intValue, err := getIntValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		//theValue.Elem().SetInt(intValue)
		theValue = reflect.ValueOf(intValue)
		return theValue, err
	}

	// Unsigned interger
	if strings.HasPrefix(theType.String(), "uint") {
		uintValue, err := getUintValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, nil
		}
		//theValue.Elem().SetUint(uintValue)
		theValue = reflect.ValueOf(uintValue)
		return theValue, err
	}

	if strings.HasPrefix(theType.String(), "float") {
		floatValue, err := getFloatValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		//theValue.Elem().SetFloat(floatValue)
		theValue = reflect.ValueOf(floatValue)
		return theValue, nil
	}

	// strings
	if theType.String() == "string" {
		stringValue, err := getStringValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		theValue = reflect.ValueOf(stringValue)
		return theValue, nil
	}
	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

func reflectValues(valueType string, value interface{}) (reflect.Value, error) {

	theType, ok := typeMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrorUnsupportedType(valueType)
	}

	if value == nil {
		return reflect.MakeSlice(reflect.SliceOf(theType), 0, 0, ), nil
	}

	var theValue reflect.Value
	if theType.String() == "[]bool" {
		bools := reflect.ValueOf(value)
		theValue = reflect.MakeSlice(reflect.SliceOf(theType), bools.Len(), bools.Len())
		for i := 0; i < bools.Len(); i++ {
			boolValue, err := getBoolValue(strings.Split(theType.String(), "[]")[1], bools.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			theValue.Index(i).SetBool(boolValue)
		}
		return theValue, nil
	}

	//Integers
	if strings.HasPrefix(theType.String(), "[]int") {
		ints := reflect.ValueOf(value)
		theValue = reflect.MakeSlice(reflect.SliceOf(theType), ints.Len(), ints.Len())
		for i := 0; i < ints.Len(); i++ {
			intValue, err := getIntValue(strings.Split(theType.String(), "[]")[1], ints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			//theValue.Index(i).SetInt(intValue)
			theValue.Index(i).SetInt(intValue)
		}
		return theValue, nil
	}

	//unsigned integers
	if strings.HasPrefix(theType.String(), "[]uint") {
		uints := reflect.ValueOf(value)
		theValue = reflect.MakeSlice(reflect.SliceOf(theType), uints.Len(), uints.Len())
		for i := 0; i < uints.Len(); i++ {
			uintValue, err := getUintValue(strings.Split(theType.String(), "[]")[1], uints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			theValue.Index(i).SetUint(uintValue)
		}
		return theValue, nil
	}

	//Floating pont number
	if strings.HasPrefix(theType.String(), "[]float") {
		floats := reflect.ValueOf(value)
		theValue = reflect.MakeSlice(reflect.SliceOf(theType), floats.Len(), floats.Len())
		for i := 0; i < floats.Len(); i++ {
			floatValue, err := getFloatValue(strings.Split(theType.String(), "[]")[1], floats.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			theValue.SetFloat(floatValue)
		}
		return theValue, nil
	}

	// strings
	if theType.String() == "[]string" {
		strs := reflect.ValueOf(value)
		theValue = reflect.MakeSlice(reflect.SliceOf(theType), strs.Len(), strs.Len())

		for i := 0; i < strs.Len(); i++ {
			strValue, err := getStringValue(strings.Split(theType.String(), "[]")[1], strs.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			theValue.Index(i).SetString(strValue)
		}
		return theValue, nil
	}
	return reflect.Value{}, NewErrorUnsupportedType(valueType)

}

func getBoolValue(theType string, value interface{}) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, typeConvError(value, typeMap[theType].String())
	}
	return b, nil
}

func getIntValue(theType string, value interface{}) (int64, error) {
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConvError(value, typeMap[theType].String())
		}
		return n.Int64()
	}

	n, ok := value.(int64)
	if !ok {
		return 0, typeConvError(value, typeMap[theType].String())
	}
	return n, nil
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	if strings.HasPrefix(fmt.Sprintf("%T"), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConvError(value, typeMap[theType].String())
		}
		intval, err := n.Int64()
		if err != nil {
			return 0, err
		}
		return uint64(intval), nil
	}
	n, ok := value.(uint64)
	if !ok {
		return 0, typeConvError(value, typeMap[theType].String())
	}
	return n, nil
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	if strings.HasPrefix(fmt.Sprintf("%T"), "json.Number") {
		f, ok := value.(json.Number)
		if !ok {
			return 0, typeConvError(value, typeMap[theType].String())
		}
		return f.Float64()
	}

	f, ok := value.(float64)
	if !ok {
		return 0, typeConvError(value, typeMap[theType].String())
	}
	return f, nil
}

func getStringValue(theType string, value interface{}) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", typeConvError(value, theType)
	}
	return s, nil
}

func IsContextType(t reflect.Type) bool {
	return t == ctxType
}
