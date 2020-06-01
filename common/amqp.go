package common

import (
	"crypto/tls"
	"fmt"

	"github.com/streadway/amqp"
)

type AMQPConnector struct{}

func (ac *AMQPConnector) Connect(url string, tlsConfig *tls.Config, exchange, exchangeType, queueName string, queueDurable, queueDelete bool, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, <-chan *amqp.Error, error) {
	conn, channel, err := ac.Open(url, tlsConfig)
	if err != nil {
		return nil, nil, amqp.Queue{}, nil, nil, err
	}
	if exchange != "" {
		if err = channel.ExchangeDeclare(
			exchange,            // name of the exchange
			exchangeType,        // type
			true,                // durable
			false,               // delete when complete
			false,               // internal
			false,               // noWait
			exchangeDeclareArgs, // arguments
		); err != nil {
			return conn, channel, amqp.Queue{}, nil, nil, fmt.Errorf("Exchange declare error: %s", err)
		}
	}
	var queue amqp.Queue
	if queueName != "" {
		queue, err = channel.QueueDeclare(
			queueName,        // name
			queueDurable,     // durable
			queueDelete,      // delete when unused
			false,            // exclusive
			false,            // no-wait
			queueDeclareArgs, // arguments
		)
		if err != nil {
			return conn, channel, amqp.Queue{}, nil, nil, fmt.Errorf("Queue declare error: %s", err)
		}
		if err = channel.QueueBind(
			queue.Name,       // name of the queue
			queueBindingKey,  // binding key
			exchange,         // source exchange
			false,            // noWait
			queueBindingArgs, // arguments
		); err != nil {
			return conn, channel, queue, nil, nil, fmt.Errorf("Queue bind error: %s", err)
		}
	}

	if err = channel.Confirm(false); err != nil {
		return conn, channel, queue, nil, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}
	return conn, channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), conn.NotifyClose(make(chan *amqp.Error, 1)), nil

}

func (ac *AMQPConnector) DeleteQueue(channel *amqp.Channel, queueName string) error {
	_, err := channel.QueueDelete(
		queueName, // name
		false,     // ifUnused
		false,     // ifEmpty
		false,     // noWait
	)
	return err
}

func (*AMQPConnector) InspectQueue(channel *amqp.Channel, queueName string) (*amqp.Queue, error) {
	queueState, err := channel.QueueInspect(queueName)
	if err != nil {
		return nil, fmt.Errorf("queue inspect error: %s", err)
	}
	return &queueState, nil
}

func (ac *AMQPConnector) Open(url string, tlsConfig *tls.Config) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialTLS(url, tlsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("dial error: %s", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel error: %s", err)
	}
	return conn, channel, nil
}

func (ac *AMQPConnector) Close(channel *amqp.Channel, conn *amqp.Connection) error {
	if channel != nil {
		if err := channel.Close(); err != nil {
			return fmt.Errorf("close channel error: %s", err)
		}
	}

	if conn != nil {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("close connection error: %s", err)
		}
	}
	return nil
}
