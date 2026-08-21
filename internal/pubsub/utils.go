package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal to json bytes: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to publish with context: %v", err)
	}
	return nil
}

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	connChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel on the conn: %v", err)
	}

	durable := false
	autoDelete := true
	exclusive := true
	if queueType == SimpleQueueDurable {
		durable = true
		autoDelete = false
		exclusive = false
	}

	queue, err := connChannel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare a queue on the conn: %v", err)
	}

	err = connChannel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind a queue on the conn: %v", err)
	}

	return connChannel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	// Declaring and binding the exchange to the queue
	connChan, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Fatalf("failed to declare and bind a queue on the channel: %v", err)
	}

	// using the channel from above, getting a amqp delivery struct to consume the incoming queue messages
	msgs, err := connChan.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("failed to consume: %v", err)
	}

	// go routine to process these messages as they come, unmarshalling it and using our handler function to handle it and acknowledging that we have successfully processed ths one
	go func(){
		defer connChan.Close()
		for msg := range msgs {
			var temp T
			err := json.Unmarshal(msg.Body, &temp)
			if err != nil {
				log.Fatalf("failed to unmarshal: %v", err)
			}
			handler(temp)
			err = msg.Ack(false)
			if err != nil {
				log.Fatalf("failed to acknowledge the process back: %v", err)
			}
		}
	}()

	return nil
}
