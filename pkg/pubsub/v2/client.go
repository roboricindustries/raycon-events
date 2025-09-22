package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// Client
// -----------------------------------------------------------------------------

type Client struct {
	conn   *amqp.Connection
	pool   *ChannelPool
	config RabbitMQConfig
	logger *slog.Logger

	consumerWG     sync.WaitGroup
	consumerClosed chan string
	consumerSpecs  map[string]ConsumerSpec
}

func (c *Client) Config() RabbitMQConfig { return c.config }

func NewClient(ctx context.Context, config RabbitMQConfig, logger *slog.Logger) (*Client, error) {
	const op = "rabbitmq.NewClient"

	if config.URL == "" {
		return nil, fmt.Errorf("rabbitmq URL is required")
	}

	if logger != nil {
		u, _ := url.Parse(config.URL)
		host := ""
		if u != nil {
			host = u.Host
		}
		logger.With("op", op).Info("connecting to rabbitmq", slog.String("host", host))
	}

	// Dial (amqp library has no ctx; we just enforce a time boundary)
	timeoutSec := config.ConnTimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 30
	}
	dialDeadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(dialDeadline) {
		dialDeadline = ctxDeadline
	}

	if time.Now().After(dialDeadline) {
		return nil, fmt.Errorf("context deadline exceeded before connection attempt")
	}

	dial := config.Dialer
	if dial == nil {
		dial = func(_ context.Context, u string) (*amqp.Connection, error) { return amqp.Dial(u) }
	}
	conn, err := dial(ctx, config.URL)
	if err != nil {
		if logger != nil {
			logger.With("op", op).Error("dial failed", slog.Any("error", err))
		}
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Declare exchanges once on a throwaway channel
	tempCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	client := &Client{
		conn:   conn,
		config: config,
		logger: logger,
	}
	if err := client.setupExchanges(tempCh); err != nil {
		tempCh.Close()
		client.Close()
		return nil, err
	}
	_ = tempCh.Close()

	// Publisher channel pool
	size := config.PublishPoolSize
	if size <= 0 {
		size = 16
	}
	pool, err := NewChannelPool(conn, size)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create channel pool: %w", err)
	}
	client.pool = pool

	if logger != nil {
		logger.With("op", op).Info("client ready")
	}
	return client, nil
}

// setupExchanges declares only exchanges here.
// Queues/bindings are per-consumer (so they can carry DLX/TTL args).
func (c *Client) setupExchanges(ch *amqp.Channel) error {
	declare := func(ex string) error {
		if ex == "" {
			return nil
		}
		return ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil)
	}
	// Known top-level exchanges
	if err := declare(c.config.DefaultGatewayExchange); err != nil {
		return fmt.Errorf("declare gateway exchange: %w", err)
	}
	if err := declare(c.config.DefaultDeliveryExchange); err != nil {
		return fmt.Errorf("declare delivery exchange: %w", err)
	}
	// Optional: user-provided extras
	for _, ex := range c.config.Exchanges {
		if ex == "" {
			continue
		}
		if err := declare(ex); err != nil {
			return fmt.Errorf("declare extra exchange %q: %w", ex, err)
		}
	}
	return nil
}

// Close stops consumers, closes pool and connection.
func (c *Client) Close() {
	done := make(chan struct{})
	go func() {
		c.consumerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	if c.pool != nil {
		c.pool.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
