# GoCelery
go语言的分布式计算系统(python中的Celery go实现)


### 项目介绍

##### 

```
项目结构
├──a-test| #接口测试文件
├── backends    # 数据结构处理代码段
├── brokers     # 消息任务队列
├── common      # 公共部分
├── config      # 项目配置代码单
├── log         # 日志
├── retry       # 任务重发处理
├── tasks       # 消息任务处理
├── tracing     #
├── server.go   # 消费者
├── worker.go   # 生产者
```



#### 项目简介 
1. 简介: Gocelery 是分布式的异步任务队列，远程调度任务的。
2. 实现方式: Gocelery 主要是通过中间人来实现远程调度的，中间Redis服务支持远程访问,生产者将任务redis消息任务，worker处理任务
3. 优点：单个的Celery进程每分钟可以处理百万级的任务，并且只需要毫秒级的往返延迟，每个部分都可以扩展使用，自定义池实现、序列化、压缩方案、日志记录、调度器、消费者、生产者、broker传输等等
4. 缺点：不适合用于有上下文逻辑处理任务，且需要在联网的环境运行（局域网除外）

#### 工作原理
![image](https://images2015.cnblogs.com/blog/720333/201701/720333-20170126182955581-1727025143.png)
#### 简单使用

##### 运行redis(其他中间件也可以)

#### worker代码
```
var cnf = &config.Config{
	Broker:        "redis://127.0.0.1:6379",
	DefaultQueue:  "machinery_tasks",
	ResultBackend: "redis://127.0.0.1:6379",
	AMQP: &config.AMQPConfig{
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		BindingKey:   "machinery_task",
	},
}

func HelloWorld(name string) (string, error) {
	return "你好" + name, nil
}

func Add(a, b int64) (int64, error) {
	return a + b, nil
}

func GoCeleryRun() error {
	server, err := GoCelery.NewServer(cnf)
	tasks := make(map[string]interface{})
	tasks["Add"] = Add
	tasks["HelloWorld"] = HelloWorld
	err = server.RegisterTasks(tasks)
	if err != nil {
		return err
	}
	worker := server.NewWorker("worker@localhost", 10)
	err = worker.Launch()
	if err != nil {
		return err
	}
	return nil
}

func TestRelay(t *testing.T) {
	GoCeleryRun()
}
```

#### client代码
```
var cnnf = &config.Config{
	Broker:        "redis://127.0.0.1:6379",
	DefaultQueue:  "machinery_tasks",
	ResultBackend: "redis://127.0.0.1:6379",
	AMQP: &config.AMQPConfig{
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		BindingKey:   "machinery_task",
	},
}

func Working() error {
	server, err := GoCelery.NewServer(cnnf)
	if err != nil {
		return err
	}
	taskss := make(map[string]interface{})
	taskss["Add"] = func(a, b int64) (int64, error) { return 65, nil }
	taskss["HelloWorld"] = func(a string) (string, error) { return "", nil }
	err = server.RegisterTasks(taskss)
	if err != nil {
		return err
	}
	signature := &tasks.Signature{
		Name: "HelloWorld",
		Args: []tasks.Arg{
			{Type: "string", Value: "国学大师",},
		},
	}
	asyncResult, err := server.SendTask(signature)
	if err != nil {
		return err
	}
	res, err := asyncResult.Get(1000)
	if err != nil {
		return err
	}
	fmt.Println("resp:", res[0])
	return nil
}

func TestClient(t *testing.T) {
	Working()
}
```




