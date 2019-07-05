package amqp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/itzujun/gocelery/brokers/errs"
	"github.com/itzujun/gocelery/brokers/iface"
	"github.com/itzujun/gocelery/common"
	"github.com/itzujun/gocelery/config"
	"github.com/itzujun/gocelery/tasks"
	"github.com/streadway/amqp"
	"sync"
)

type AMQPConnection struct {
	queueName    string
	connection   *amqp.Connection
	channel      *amqp.Channel
	queue        amqp.Queue
	confirmation <-chan amqp.Confirmation
	errorchan    <-chan *amqp.Error
	cleanup      chan struct{}
}

type Broker struct {
	common.Broker
	common.AMQPConnector
	processingWG     sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	connections      map[string]*AMQPConnection
	connectionsMutex sync.RWMutex
}

func New(cnf *config.Config) iface.Broker {
	return &Broker{Broker: common.NewBroker(cnf), AMQPConnector: common.AMQPConnector{}, connections: make(map[string]*AMQPConnection)}
}

func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	//todo
	return nil, errors.New("")
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)
	queueName := taskProcessor.CustomQueue()
	if queueName == "" {
		queueName = b.GetConfig().DefaultQueue
	}
	conn, channel, queue, _, amqpCloseChan, err := b.Connect(
		b.GetConfig().Broker,
		b.GetConfig().TLSConfig,
		b.GetConfig().AMQP.Exchange,                     // exchange name
		b.GetConfig().AMQP.ExchangeType,                 // exchange type
		queueName,                                       // queue name
		true,                                            // queue durable
		false,                                           // queue delete when unused
		b.GetConfig().AMQP.BindingKey,                   // queue binding key
		nil,                                             // exchange declare args
		nil,                                             // queue declare args
		amqp.Table(b.GetConfig().AMQP.QueueBindingArgs), // queue binding args
	)
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		return b.GetRetry(), err
	}
	defer b.Close(channel, conn)
	if err = channel.Qos(
		b.GetConfig().AMQP.PrefetchCount,
		0,     // prefetch size
		false, // global
	); err != nil {
		return b.GetRetry(), fmt.Errorf("channel qos error: %s", err)
	}
	deliveries, err := channel.Consume(
		queue.Name,  // queue
		consumerTag, // consumer tag
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return b.GetRetry(), fmt.Errorf("queue consume error: %s", err)
	}
	fmt.Println("[*] Waiting for messages. To exit press CTRL+C")
	if err := b.consume(deliveries, concurrency, taskProcessor, amqpCloseChan); err != nil {
		return b.GetRetry(), err
	}
	b.processingWG.Wait()
	return b.GetRetry(), nil
}

func (b *Broker) consume(deliveries <-chan amqp.Delivery, concurrency int, taskProcessor iface.TaskProcessor, amqpCloseChan <-chan *amqp.Error) error {
	pool := make(chan struct{}, concurrency)
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()
	errorsChan := make(chan error)
	for {
		select {
		case amqpErr := <-amqpCloseChan:
			return amqpErr
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				<-pool
			}
			b.processingWG.Add(1)
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}
				b.processingWG.Done()
				if concurrency > 0 {
					pool <- struct{}{}
				}
			}()
		case <-b.GetStopChan():
			return nil
		}
	}
}

func (b *Broker) consumeOne(delivery amqp.Delivery, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Body) == 0 {
		delivery.Nack(true, false)                     // multiple, requeue
		return errors.New("received an empty message") // RabbitMQ down?
	}
	var multiple, requeue = false, false
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		delivery.Nack(multiple, requeue)
		return errs.NewErrCouldNotUnmarshaTaskSignature(delivery.Body, err)
	}
	if !b.IsTaskRegistered(signature.Name) {
		if !delivery.Redelivered {
			requeue = true
			fmt.Printf("task not registered with this worker. Requeing message: %s", delivery.Body)
		}
		delivery.Nack(multiple, requeue)
		return nil
	}
	fmt.Printf("Received new message: %s", delivery.Body)
	err := taskProcessor.Process(signature)
	delivery.Ack(multiple)
	return err
}

func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	b.processingWG.Wait()
}

func (b *Broker) GetOrOpenConnection(queueName string, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*AMQPConnection, error) {
	var err error
	b.connectionsMutex.Lock()
	defer b.connectionsMutex.Unlock()
	conn, ok := b.connections[queueName]
	if !ok {
		conn = &AMQPConnection{
			queueName: queueName,
			cleanup:   make(chan struct{}),
		}
		conn.connection, conn.channel, conn.queue, conn.confirmation, conn.errorchan, err = b.Connect(
			b.GetConfig().Broker,
			b.GetConfig().TLSConfig,
			b.GetConfig().AMQP.Exchange,     // exchange name
			b.GetConfig().AMQP.ExchangeType, // exchange type
			queueName,                       // queue name
			true,                            // queue durable
			false,                           // queue delete when unused
			queueBindingKey,                 // queue binding key
			exchangeDeclareArgs,             // exchange declare args
			queueDeclareArgs,                // queue declare args
			queueBindingArgs,                // queue binding args
		)
		if err != nil {
			return nil, err
		}

		// Reconnect to the channel if it disconnects/errors out
		go func() {
			select {
			case err = <-conn.errorchan:
				fmt.Printf("Error occured on queue: %s. Reconnecting", queueName)
				_, err := b.GetOrOpenConnection(queueName, queueBindingKey, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs)
				if err != nil {
					fmt.Printf("Failed to reopen queue: %s.", queueName)
				}
			case <-conn.cleanup:
				return
			}
			return
		}()
		b.connections[queueName] = conn
	}
	return conn, nil
}

func (b *Broker) CloseConnections() error {
	b.connectionsMutex.Lock()
	defer b.connectionsMutex.Unlock()

	for key, conn := range b.connections {
		if err := b.Close(conn.channel, conn.connection); err != nil {
			fmt.Println("Failed to close channel")
			return nil
		}
		close(conn.cleanup)
		delete(b.connections, key)
	}
	return nil
}
