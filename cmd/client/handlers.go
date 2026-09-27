package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, chn *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(am)
		if moveOutcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		} else if moveOutcome == gamelogic.MoveOutcomeMakeWar {
			username := gs.GetPlayerSnap().Username
			key := routing.WarRecognitionsPrefix + "." + username
			err := pubsub.PublishJSON(chn, routing.ExchangePerilTopic, key, gamelogic.RecognitionOfWar{
				Attacker: am.Player ,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
			
		} else {
			return pubsub.NackDiscard
		}

	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, _,_ := gs.HandleWar(rw)
		if warOutcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackRequeue
		} else if warOutcome == gamelogic.WarOutcomeNoUnits{
			return pubsub.NackDiscard
		} else if warOutcome == gamelogic.WarOutcomeOpponentWon {
			return pubsub.Ack
		} else if warOutcome == gamelogic.WarOutcomeYouWon {
			return pubsub.Ack
		} else if warOutcome == gamelogic.WarOutcomeDraw {
			return pubsub.Ack
		} else {
			fmt.Println("War outcome error")
			return pubsub.NackDiscard
		}
		
	}
}
