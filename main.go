package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type (
	ExamplePart string
	Config      struct {
		Example ExamplePart
		Workers int
	}
	ProducerFunc func(ctx context.Context, conn *amqp.Connection) error
	ConsumerFunc func(ctx context.Context, conn *amqp.Connection) error
)

var (
	Base    ExamplePart = "base"
	Queues  ExamplePart = "queues"
	PubSub  ExamplePart = "pubsub"
	Routing ExamplePart = "routing"
	Topics  ExamplePart = "topics"
	RPC     ExamplePart = "rpc"
)

func main() {
	config := parseConfig()
	conn := initConnection()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err := startProducersAndConsumers(ctx, conn, config)
	if err != nil {
		log.Fatalln(err)
	}
}

func startProducersAndConsumers(
	ctx context.Context,
	conn *amqp.Connection,
	config *Config,
) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	switch config.Example {
	case Base:
		err = startConsumers(ctx, conn, baseProducer, baseConsumer, config.Workers)
	//case Queues:
	//	queuesProducer(conn)
	//	startConsumers(context.Background(), queuesConsumer, config.Workers)
	//case PubSub:
	//	pubsubProducer(conn)
	//	startConsumers(context.Background(), pubsubConsumer, config.Workers)
	//case Routing:
	//	routerProducer(conn)
	//	startConsumers(context.Background(), routerConsumer, config.Workers)
	//case Topics:
	//	topicsProducer(conn)
	//	startConsumers(context.Background(), topicsConsumer, config.Workers)
	//case RPC:
	//	rpcProducer(conn)
	//	startConsumers(context.Background(), rpcConsumer, config.Workers)
	default:
		log.Fatalln("Wtf, i've implemented validation...")
	}

	return nil
}

func startConsumers(ctx context.Context, conn *amqp.Connection, p ProducerFunc, c ConsumerFunc, workerAmount int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p(ctx, conn)
	})
	for i := 0; i < workerAmount; i++ {
		g.Go(func() error {
			return c(ctx, conn)
		})
	}

	return g.Wait()
}

func queuesProducer(conn *amqp.Connection)                      {}
func queuesConsumer(ctx context.Context, conn *amqp.Connection) {}

func pubsubProducer(conn *amqp.Connection)                      {}
func pubsubConsumer(ctx context.Context, conn *amqp.Connection) {}

func routerProducer(conn *amqp.Connection)                      {}
func routerConsumer(ctx context.Context, conn *amqp.Connection) {}

func topicsProducer(conn *amqp.Connection)                      {}
func topicsConsumer(ctx context.Context, conn *amqp.Connection) {}

func rpcProducer(conn *amqp.Connection)                      {}
func rpcConsumer(ctx context.Context, conn *amqp.Connection) {}
