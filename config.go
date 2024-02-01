package main

import (
	"errors"
	"flag"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func initConnection() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatalln("Could not initialize connection: ", err)
	}
	return conn
}

func validateExamplePart(part string) (ExamplePart, error) {
	switch ExamplePart(part) {
	case Base:
		return Base, nil
	case Queues:
		return Queues, nil
	case PubSub:
		return PubSub, nil
	case Routing:
		return Routing, nil
	case Topics:
		return Topics, nil
	case RPC:
		return RPC, nil
	default:
		return "", errors.New("invalid example provided")
	}
}

func parseConfig() *Config {
	part := flag.String("example", "base", "Allowed: base, queues, pubsub, routing, topics, rpc")
	workerAmount := flag.Int("workers", 1, "Amount of workers to start, min: 1")

	flag.Parse()

	if part == nil {
		log.Fatalln("Part to run must be specified, using default")
	}
	if workerAmount == nil {
		log.Println("Amount of workers is not specified, using default")
	}

	partToRun, err := validateExamplePart(*part)
	if err != nil {
		log.Fatalln(err)
	}
	if *workerAmount < 1 {
		log.Fatalln("Worker amount must be greater than 1")
	}

	return &Config{
		Example: partToRun,
		Workers: *workerAmount,
	}
}
