package pubsub

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

type Publisher interface {
	Publish(ctx context.Context, key string, msg common.Envelope) error
	Close() error
}

type PublisherOptions struct {
	Exchange        string // required
	ExchangeType    string // default "topic"
	DeclareExchange bool   // if false, assumes exchange already exists
}

type rmqClient struct {
	conn     *amqp091.Connection
	exchange string
	log      *slog.Logger
}

func NewPublisher(conn *amqp091.Connection, logger *slog.Logger, opts PublisherOptions) (Publisher, error) {
	if opts.ExchangeType == "" {
		opts.ExchangeType = "topic"
	}

	if opts.Exchange != "" && opts.DeclareExchange {
		ch, err := conn.Channel()
		if err != nil {
			return nil, err
		}
		if err := ch.ExchangeDeclare(opts.Exchange, opts.ExchangeType, true, false, false, false, nil); err != nil {
			_ = ch.Close()
			return nil, err
		}
		_ = ch.Close()
	}
	return &rmqClient{
		conn:     conn,
		log:      logger,
		exchange: opts.Exchange,
	}, nil
}

func (r *rmqClient) Publish(ctx context.Context, key string, msg common.Envelope) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if msg.Meta.ID == "" {
		msg.Meta.ID = uuid.NewString()
	}
	if msg.Meta.CorrelationID == "" {
		msg.Meta.CorrelationID = uuid.NewString()
	}
	if msg.Meta.Time.IsZero() {
		msg.Meta.Time = time.Now()
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		ctx, r.exchange, key, false, false,
		amqp091.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp091.Persistent,
			MessageId:     msg.Meta.ID,
			CorrelationId: msg.Meta.CorrelationID,
			Timestamp:     msg.Meta.Time,
			Body:          body,
		},
	)
	if err == nil {
		r.log.Info("published", slog.String("key", key), slog.String("exchange", r.exchange))
	}
	return err
}

func (e *rmqClient) Close() error {
	return nil
}
