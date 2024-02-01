package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	queueBaseMsg = "queue id "
)

func initQueueQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
}

func queuesProducer(ctx context.Context, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := initQueueQueue(ch)
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		msg := []byte(queueBaseMsg + strconv.FormatInt(int64(i), 10))
		err = ch.PublishWithContext(
			ctx,
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "plain/text",
				Body:        msg,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func queuesConsumer(ctx context.Context, conn *amqp.Connection, consumerId int) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := initQueueQueue(ch)
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

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgs:
			splitMsg := strings.Split(string(msg.Body), " ")
			msgId, err := strconv.ParseInt(splitMsg[len(splitMsg)-1], 10, 64)
			if err != nil {
				return err
			}

			log.Println("Received msg: ", msgId, ", by:", consumerId)
			time.Sleep(time.Second * time.Duration(msgId))
		}
	}
}

var (
	_ ProducerFunc = queuesProducer
	_ ConsumerFunc = queuesConsumer
)
