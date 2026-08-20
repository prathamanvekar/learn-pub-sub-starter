package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	connChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not open a channel to the connection: %v", err)
	}

	key := routing.GameLogSlug + ".*"

	_, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, key, pubsub.SimpleQueueDurable)
	if err != nil {
		log.Fatalf("failed to declare and bind a queue on the channel: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	for true {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		if words[0] == "pause" {
			fmt.Println("Sending a pause message!")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatalf("failed to publish json: %v", err)
			}

		} else if words[0] == "resume" {
			fmt.Println("Sending a resume message!")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatalf("failed to publish json: %v", err)
			}

		} else if words[0] == "quit" {
			fmt.Println("Exiting!")
			break

		} else {
			fmt.Println("me no hablo engles")
		}

	}

}
