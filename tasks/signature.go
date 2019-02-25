package tasks

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)

type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

//网络信息头
type Headers map[string]interface{}

func (h Headers) Set(key, value string) {
	h[key] = value
}

func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		stringValue, ok := v.(string)
		if !ok {
			continue
		}
		if err := handler(k, stringValue); err != nil {
			return err
		}
	}
	return nil
}

//new task signature
type Signature struct {
	UUID                 string
	Name                 string
	RouteKey             string
	ETA                  *time.Time
	GroupUUID            string
	GroupTaskCount       int
	Args                 []Arg
	Immutable            bool
	RetryCount           int
	RetryTimeout         int
	OnSuccess            []*Signature
	OnError              []*Signature
	ChordCallback        *Signature
	BrokerMessageGroupId string
}

func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil

}
