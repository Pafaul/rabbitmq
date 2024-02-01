package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

var (
	baseBody = []byte("Hello there! Obi-Wan Kenobi?")
)

func initBaseQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
}

func baseProducer(ctx context.Context, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := initBaseQueue(ch)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        baseBody,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func baseConsumer(ctx context.Context, conn *amqp.Connection, consumerId int) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := initBaseQueue(ch)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

mainLoop:
	for {
		select {
		case <-ctx.Done():
			break mainLoop
		case msg := <-msgs:
			received := msg.Body
			log.Println("Consumer id:", consumerId)
			log.Println("Received msg:", string(received))
			break mainLoop
		}
	}

	return err
}

var (
	_ ProducerFunc = baseProducer
	_ ConsumerFunc = baseConsumer
)
