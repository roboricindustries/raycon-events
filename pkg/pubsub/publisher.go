package pubsub

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	Publish(ctx context.Context, key string, msg any) error
	Close() error
}

type rmqClient struct {
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	exchange string
}

func New(url, exchange string) (Publisher, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
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
		ch:       ch,
		exchange: exchange,
	}, nil

}

func (r *rmqClient) Publish(ctx context.Context, key string, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ack := r.ch.NotifyPublish(make(chan amqp091.Confirmation, 1))

	if err := r.ch.PublishWithContext(
		ctx, r.exchange, key, false, false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    uuid.NewString(),
			Timestamp:    time.Now(),
			Body:         body,
		},
	); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c := <-ack:
		if !c.Ack {
			return amqp091.ErrClosed
		}
		return nil
	}
}

func (e *rmqClient) Close() error {
	_ = e.ch.Close()
	return e.conn.Close()
}
