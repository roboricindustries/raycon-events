package pubsub

import (
	"errors"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type RetryConfig struct {
	Channel *amqp091.Channel

	DeadExchange  string
	FinalExchange string
	RetryExchange string

	DeadQueue  string
	FinalQueue string

	TTL time.Duration
}

func SetupRetryInfra(cfg *RetryConfig) error {
	if cfg == nil {
		return errors.New("empty config")
	}
	if err := cfg.Channel.ExchangeDeclare(cfg.DeadExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := cfg.Channel.ExchangeDeclare(cfg.RetryExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := cfg.Channel.ExchangeDeclare(cfg.FinalExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	args := amqp091.Table{
		"x-message-ttl":          int32(cfg.TTL.Milliseconds()),
		"x-dead-letter-exchange": cfg.RetryExchange,
	}

	dq, err := cfg.Channel.QueueDeclare(cfg.DeadQueue, true, false, false, false, args)
	if err != nil {
		return err
	}
	if err := cfg.Channel.QueueBind(
		dq.Name, "#", cfg.DeadExchange, false, nil,
	); err != nil {
		return err
	}

	fq, err := cfg.Channel.QueueDeclare(cfg.FinalQueue, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := cfg.Channel.QueueBind(
		fq.Name, "#", cfg.FinalExchange, false, nil,
	); err != nil {
		return err
	}
	return nil
}
