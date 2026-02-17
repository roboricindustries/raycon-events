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

	_, pool, _, _ := c.snapshot()
	if pool == nil {
		return fmt.Errorf("publisher pool is not initialized")
	}

	leaseCh, err := pool.Lease(ctx)
	if err != nil {

		return fmt.Errorf("borrow channel: %w", err)
	}
	defer leaseCh.Release()

	pubErr := leaseCh.Channel().PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		MessageId:     env.Meta.ID,
		CorrelationId: env.Meta.CorrelationID,
		Type:          env.Meta.Type,
		Timestamp:     env.Meta.Time,
		AppId:         FirstNonEmpty(c.config.Queues["producer"], ""),
	})
	if pubErr != nil {
		leaseCh.Discard()
		return pubErr
	}
	return nil
}

// NotifyChan provides a confirm-enabled channel to the callback.
// IMPORTANT: this uses a fresh channel (NOT the publisher pool) to avoid leaking NotifyPublish listeners.
func (c *Client) NotifyChan(ctx context.Context, withChan func(ch *amqp.Channel, confirms <-chan amqp.Confirmation) error) error {
	return c.WithConfirmChan(ctx, withChan)
}

func (c *Client) WithConfirmChan(
	ctx context.Context,
	fn func(ch *amqp.Channel, confirms <-chan amqp.Confirmation) error,
) error {
	conn, _, _, _ := c.snapshot()
	if conn == nil {
		return fmt.Errorf("nil connection")
	}

	// Use a fresh channel each time so confirms/listeners don't leak.
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer SafeClose(ch)

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm mode: %w", err)
	}

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
		AppId:         FirstNonEmpty(c.config.Queues["producer"], ""),
	})
}
