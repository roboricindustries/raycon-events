package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

// PublishJSON publishes an Envelope as JSON with proper AMQP headers.
func (c *Client) PublishJSON(ctx context.Context, exchange, routingKey string, env common.Envelope) error {
	if exchange == "" {
		exchange = c.config.DefaultDeliveryExchange
	}
	if routingKey == "" {
		routingKey = c.config.DefaultDeliverRoutingKey
	}

	// Ensure metadata consistency
	if env.Meta.ID == "" {
		return fmt.Errorf("envelope.Meta.ID is required")
	}
	if env.Meta.CorrelationID == "" {
		env.Meta.CorrelationID = env.Meta.ID // fallback to ID if no correlation
	}
	if env.Meta.Time.IsZero() {
		env.Meta.Time = time.Now().UTC()
	}

	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	ch, err := c.pool.Borrow(ctx, c.config.PoolRetryDelayMs)
	if err != nil {
		return fmt.Errorf("borrow channel: %w", err)
	}
	defer c.pool.Return(ch)

	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		MessageId:     env.Meta.ID,
		CorrelationId: env.Meta.CorrelationID,
		Type:          env.Meta.Type,
		Timestamp:     env.Meta.Time,
		AppId:         FirstNonEmpty(c.config.Queues["producer"], ""), // or c.config.DefaultProducer
	})
}

func (c *Client) NotifyChan(ctx context.Context, withChan func(ch *amqp.Channel, confirms <-chan amqp.Confirmation) error) error {
	ch, err := c.pool.Borrow(ctx, c.config.PoolRetryDelayMs)
	if err != nil {
		return fmt.Errorf("borrow channel: %w", err)
	}
	defer c.pool.Return(ch)
	if err := ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	err = withChan(ch, confirms)
	return err
}

func (c *Client) WithConfirmChan(
	ctx context.Context,
	fn func(ch *amqp.Channel, confirms <-chan amqp.Confirmation) error,
) error {
	ch, err := c.pool.Borrow(ctx, c.config.PoolRetryDelayMs)
	if err != nil {
		return fmt.Errorf("borrow channel: %w", err)
	}

	defer c.pool.Return(ch)

	// Turn on confirm mode (idempotent on a fresh channel)
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm mode: %w", err)
	}

	// Big enough buffer to hold a batch’s confirms; adjust if you batch > 1024 msgs
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1024))

	return fn(ch, confirms)
}

func (c *Client) RawPublish(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, env common.Envelope) error {
	if exchange == "" {
		exchange = c.config.DefaultDeliveryExchange
	}
	if routingKey == "" {
		routingKey = c.config.DefaultDeliverRoutingKey
	}

	// Ensure metadata consistency
	if env.Meta.ID == "" {
		return fmt.Errorf("envelope.Meta.ID is required")
	}
	if env.Meta.CorrelationID == "" {
		env.Meta.CorrelationID = env.Meta.ID // fallback to ID if no correlation
	}
	if env.Meta.Time.IsZero() {
		env.Meta.Time = time.Now().UTC()
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		MessageId:     env.Meta.ID,
		CorrelationId: env.Meta.CorrelationID,
		Type:          env.Meta.Type,
		Timestamp:     env.Meta.Time,
		AppId:         FirstNonEmpty(c.config.Queues["producer"], ""), // or c.config.DefaultProducer
	})
}
