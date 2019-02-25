package tasks

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
