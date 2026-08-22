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
	fmt.Println("Peril game client connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get username: %v", err)
	}

	pauseQueueName := routing.PauseKey + "." + username

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gs))
	if err != nil {
		log.Fatalf("failed to subscribe the pause queue on the channel: %v", err)
	}

	armyMovesQueueName := routing.ArmyMovesPrefix + "." + username
	armyMovesRoutingKey := routing.ArmyMovesPrefix + ".*"

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyMovesQueueName, armyMovesRoutingKey, pubsub.SimpleQueueTransient, handlerMove(gs))
	if err != nil {
		log.Fatalf("failed to subscribe the army move queue on the channel: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			res, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// TODO: publish the move
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, armyMovesRoutingKey, res)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(res.Units), res.ToLocation)

		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
