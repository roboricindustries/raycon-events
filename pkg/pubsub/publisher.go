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

type rmqClient struct {
	conn     *amqp091.Connection
	exchange string
	log      *slog.Logger
}

func New(url, exchange string, logger *slog.Logger) (Publisher, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(
		exchange, "topic", true, false, false, false, nil,
	); err != nil {
		conn.Close()
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return nil, err
	}

	return &rmqClient{
		conn:     conn,
		exchange: exchange,
		log:      logger,
	}, nil

}

func (r *rmqClient) Publish(ctx context.Context, key string, msg common.Envelope) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	msgID := msg.Meta.ID
	if msgID == "" {
		msgID = uuid.NewString()
	}
	cid := ""
	if msg.Meta.CorrelationID != nil {
		cid = *msg.Meta.CorrelationID
	} else {
		tmp := uuid.NewString()
		cid = tmp
	}

	err = ch.PublishWithContext(
		ctx, r.exchange, key, false, false,
		amqp091.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp091.Persistent,
			MessageId:     msgID,
			CorrelationId: cid,
			Timestamp:     time.Now(),
			Body:          body,
		},
	)
	if err == nil {
		r.log.Info("published", slog.String("key", key), slog.String("exchange", r.exchange))
	}
	return err
}

func (e *rmqClient) Close() error {
	return e.conn.Close()
}
