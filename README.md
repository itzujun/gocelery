# GoCelery
go语言的分布式计算系统(python中的Celery go实现)


### 项目介绍

##### 


#### 工作原理
![image](https://images2015.cnblogs.com/blog/720333/201701/720333-20170126182955581-1727025143.png)



```
项目结构
├──a-test| #接口定义文件
         | grpcfile.pb.go # 命令生成接口文件
         | grpcfile.proto # 接口自定义文件
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


