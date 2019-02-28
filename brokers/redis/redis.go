package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/redsync"
	"github.com/garyburd/redigo/redis"
	"github.com/itzujun/GoCelery/brokers/iface"
	"github.com/itzujun/GoCelery/common"
	"github.com/itzujun/GoCelery/config"
	"github.com/itzujun/GoCelery/tasks"
	"sync"
	"time"
)

var redisDelayedTasksKey = "delayed_tasks"

type Broker struct {
	common.Broker
	common.RedisConnector
	host              string
	password          string
	db                int
	pool              *redis.Pool
	stopReceivingChan chan int
	stopDelayedChan   chan int
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	delayedWG         sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
}

func New(cnf *config.Config, host, password, socketPath string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath
	return b
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	b.pool = nil
	conn := b.open()
	defer conn.Close()
	defer b.pool.Close()

	_, err := conn.Do("PING")
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		return b.GetRetry(), err
	}

	b.stopReceivingChan = make(chan int)
	b.stopDelayedChan = make(chan int)
	b.receivingWG.Add(1)
	b.delayedWG.Add(1)

	deliveries := make(chan []byte)
	pool := make(chan struct{}, concurrency)

	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	var concurrencyAvailable = func() bool {
		return concurrency == 0 || (len(pool)-len(deliveries) > 0)
	}

	var (
		timerDuration = time.Duration(100000000 * time.Nanosecond) // 100 miliseconds
		timer         = time.NewTimer(0)
	)

	go func() {
		defer b.receivingWG.Done()
		fmt.Println("[*] Waiting for messages. To exit press CTRL+C")
		for {
			select {
			case <-b.stopReceivingChan:
				return
			case <-timer.C:
				if concurrencyAvailable() {
					task, err := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
					if err != nil {
						timer.Reset(timerDuration)
						continue
					}
					deliveries <- task
				}
				if concurrencyAvailable() {
					timer.Reset(0)
				} else {
					timer.Reset(timerDuration)
				}
			}
		}
	}()

	go func() {
		defer b.delayedWG.Done()
		for {
			select {
			//
			case <-b.stopDelayedChan:
				return
			default:
				task, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}
				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, pool, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
	// Stop the delayed tasks goroutine
	b.stopDelayedChan <- 1
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	b.Broker.StopConsuming()
	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

func (b *Broker) Publish(signature *tasks.Signature) error {
	b.Broker.AdjustRoutingKey(signature)
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	conn := b.open()
	defer conn.Close()

	if signature.ETA != nil {
		now := time.Now().UTC()
		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", redisDelayedTasksKey, score, msg)
			return err
		}
	}
	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

func (b *Broker) consume(deliveries <-chan []byte, pool chan struct{}, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				<-pool
			}
			b.processingWG.Add(1)
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.Broker.GetStopChan():
			return nil
		}
	}
}

func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshaTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		names := b.GetRegisteredTaskNames()
		fmt.Println("tasknames:", names)
		fmt.Println("未注册消息...", signature.Name)
		conn := b.open()
		defer conn.Close()

		conn.Do("RPUSH", getQueue(b.GetConfig(), taskProcessor), delivery)
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, 1))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items [][]byte
		reply interface{}
	)

	var pollPeriod = 20 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		pollPeriod = b.GetConfig().Redis.DelayedTasksPollPeriod
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		if _, err = conn.Do("WATCH", key); err != nil {
			return
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		conn.Send("MULTI")
		conn.Send("ZREM", key, items[0])
		reply, err = conn.Do("EXEC")
		if err != nil {
			return
		}

		if reply != nil {
			result = items[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *Broker) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
	}
	//todo
	//if b.redsync == nil {
	//	var pools = []redsync.Pool{b.pool}
	//	b.redsync = redsync.New(pools)
	//}
	return b.pool.Get()
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
