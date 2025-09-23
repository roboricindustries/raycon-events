package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// Consumer model (generic, supervised)
// -----------------------------------------------------------------------------

// RetrySpec configures the DLX-based retry pipeline.
type RetrySpec struct {
	Enabled     bool
	TTL         time.Duration
	MaxAttempts int

	DeadExchange  string
	DeadQueue     string
	FinalExchange string
	FinalQueue    string
}

// ConsumerSpec defines a single consumer.
type ConsumerSpec struct {
	Name         string
	Exchange     string // main exchange to bind
	ExchangeKind string // kind of exchange default: topic
	Queue        string
	BindingKey   string // routing key for main bind & requeue
	Prefetch     int    // 0 => use global default
	Retry        *RetrySpec

	// If true, poison messages are published to final DLQ then Acked.
	// If false, poison messages are just Acked (no copy kept).
	PoisonToFinal bool

	Consume func(ctx context.Context, d amqp.Delivery) error
}

// ErrPoison indicates non-retriable "bad content" (e.g., JSON decode fail).
var ErrPoison = errors.New("poison message")

// JSONHandler wraps a typed handler and turns JSON decode failure into ErrPoison.
func JSONHandler[T any](h func(context.Context, T) error) func(context.Context, amqp.Delivery) error {
	return func(ctx context.Context, d amqp.Delivery) error {
		var v T
		if err := json.Unmarshal(d.Body, &v); err != nil {
			return ErrPoison
		}
		return h(ctx, v)
	}
}

func (c *Client) RunWithConsumers(ctx context.Context, specs ...ConsumerSpec) error {
	c.consumerClosed = make(chan string, len(specs)*2)
	c.consumerSpecs = make(map[string]ConsumerSpec, len(specs))

	for _, s := range specs {
		c.consumerSpecs[s.Name] = s
		if err := c.startConsumer(ctx, s); err != nil {
			return fmt.Errorf("start %s: %w", s.Name, err)
		}
	}

	errCh := c.conn.NotifyClose(make(chan *amqp.Error, 1))
	base := Dsec(c.config.ReconnectBackoffBaseSeconds, 1)
	capd := Dsec(c.config.ReconnectBackoffCapSeconds, 30)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case name := <-c.consumerClosed:
			if s, ok := c.consumerSpecs[name]; ok {
				if err := c.startConsumer(ctx, s); err != nil && c.logger != nil {
					c.logger.Error("restart consumer failed", slog.String("name", name), slog.Any("error", err))
				}
			}

		case err, ok := <-errCh:
			if !ok {
				err = &amqp.Error{Reason: "connection closed"}
			}
			if c.logger != nil {
				c.logger.Error("amqp connection closed, reconnecting", slog.Any("error", err))
			}
			// reconnect loop
			backoff := base
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if rerr := c.reconnect(ctx); rerr != nil {
					wait := JitteredDelay(backoff, capd, c.config.ReconnectJitterPercent)
					if c.logger != nil {
						c.logger.Error("reconnect failed", slog.Any("error", rerr), slog.Duration("retry_in", wait))
					}
					time.Sleep(wait)
					if backoff*2 < capd {
						backoff *= 2
					}
					continue
				}

				// success → restart all consumers on new conn
				for _, s := range c.consumerSpecs {
					if err := c.startConsumer(ctx, s); err != nil && c.logger != nil {
						c.logger.Error("restart consumer after reconnect failed", slog.String("name", s.Name), slog.Any("error", err))
					}
				}
				errCh = c.conn.NotifyClose(make(chan *amqp.Error, 1))
				break
			}
		}
	}
}

// startConsumer declares the per-consumer topology and runs the loop.
func (c *Client) startConsumer(ctx context.Context, spec ConsumerSpec) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}

	pf := spec.Prefetch
	if pf <= 0 {
		pf = c.config.ConsumerPrefetch
		if pf <= 0 {
			pf = 1
		}
	}
	if err := ch.Qos(pf, 0, false); err != nil {
		_ = ch.Close()
		return err
	}

	if err := c.declareConsumerTopology(ch, spec); err != nil {
		_ = ch.Close()
		return err
	}

	msgs, err := ch.Consume(spec.Queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return err
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	c.consumerWG.Add(1)
	go func() {
		defer c.consumerWG.Done()
		for {
			select {
			case <-ctx.Done():
				_ = ch.Close()
				return

			case <-closeCh:
				// best-effort drain pending deliveries to requeue faster
				for {
					select {
					case d, ok := <-msgs:
						if !ok {
							goto drained
						}
						_ = d.Nack(false, true)
					default:
						goto drained
					}
				}
			drained:
				select {
				case c.consumerClosed <- spec.Name:
				default:
				}
				_ = ch.Close()
				return

			case d, ok := <-msgs:
				if !ok {
					_ = ch.Close()
					return
				}

				// Check max attempts for main queue (if retry enabled)
				if spec.Retry != nil && spec.Retry.Enabled && spec.Retry.MaxAttempts > 0 {
					if DeathCount(d, spec.Queue) >= spec.Retry.MaxAttempts {
						_ = PublishFinal(ch, FirstNonEmpty(spec.Retry.FinalExchange, spec.Queue+".final"), d)
						_ = d.Ack(false)
						continue
					}
				}

				err := spec.Consume(ctx, d)
				switch {
				case errors.Is(err, ErrPoison):
					// Poison policy
					if spec.PoisonToFinal {
						finalEx := FirstNonEmpty(TryFinalEx(spec), spec.Queue+".final")
						_ = PublishFinal(ch, finalEx, d)
					}
					_ = d.Ack(false)
					continue

				case err != nil:
					// Transient: use DLX if enabled, else requeue
					if spec.Retry != nil && spec.Retry.Enabled {
						_ = d.Nack(false, false) // to DLX
					} else {
						_ = d.Nack(false, true) // immediate requeue (legacy)
					}
					continue

				default:
					_ = d.Ack(false)
				}
			}
		}
	}()

	if c.logger != nil {
		c.logger.Info("consumer started", slog.String("name", spec.Name), slog.String("queue", spec.Queue), slog.Int("prefetch", pf))
	}
	return nil
}

// declareConsumerTopology declares main queue/bind, DLX/TTL queue, and final queue.
func (c *Client) declareConsumerTopology(ch *amqp.Channel, s ConsumerSpec) error {
	// Declare exchange
	exKind := s.ExchangeKind
	if exKind == "" {
		exKind = "topic"
	}
	if err := ch.ExchangeDeclare(s.Exchange, exKind, true, false, false, false, nil); err != nil {
		return err
	}
	// Main queue (optionally DLX to deadEx)
	mainArgs := amqp.Table{}
	if s.Retry != nil && s.Retry.Enabled {
		deadEx := FirstNonEmpty(s.Retry.DeadExchange, s.Queue+".dead")
		mainArgs["x-dead-letter-exchange"] = deadEx
	}
	if _, err := ch.QueueDeclare(s.Queue, true, false, false, false, mainArgs); err != nil {
		return err
	}
	if err := ch.QueueBind(s.Queue, s.BindingKey, s.Exchange, false, nil); err != nil {
		return err
	}

	needFinal := (s.Retry != nil && s.Retry.Enabled) || s.PoisonToFinal

	// DLX/TTL retry stage
	if s.Retry != nil && s.Retry.Enabled {
		deadEx := FirstNonEmpty(s.Retry.DeadExchange, s.Queue+".dead")
		deadQ := FirstNonEmpty(s.Retry.DeadQueue, s.Queue+".dead")
		if err := ch.ExchangeDeclare(deadEx, "fanout", true, false, false, false, nil); err != nil {
			return err
		}
		ttl := int32(s.Retry.TTL / time.Millisecond)
		dArgs := amqp.Table{
			"x-message-ttl":             ttl,
			"x-dead-letter-exchange":    s.Exchange,
			"x-dead-letter-routing-key": s.BindingKey,
		}
		if _, err := ch.QueueDeclare(deadQ, true, false, false, false, dArgs); err != nil {
			return err
		}
		if err := ch.QueueBind(deadQ, "", deadEx, false, nil); err != nil {
			return err
		}
	}

	// Final DLQ (for exhausted retries and/or poison)
	if needFinal {
		finalEx := FirstNonEmpty(TryFinalEx(s), s.Queue+".final")
		finalQ := FirstNonEmpty(TryFinalQ(s), s.Queue+".final")
		if err := ch.ExchangeDeclare(finalEx, "fanout", true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(finalQ, true, false, false, false, nil); err != nil {
			return err
		}
		if err := ch.QueueBind(finalQ, "", finalEx, false, nil); err != nil {
			return err
		}
	}

	return nil
}

// Reconnect the whole stack and re-declare exchanges.
func (c *Client) reconnect(ctx context.Context) error {
	const op = "rabbitmq.reconnect"

	if c.pool != nil {
		c.pool.Close()
	}
	if c.conn != nil && !c.conn.IsClosed() {
		_ = c.conn.Close()
	}

	dial := c.config.Dialer
	if dial == nil {
		dial = func(_ context.Context, u string) (*amqp.Connection, error) { return amqp.Dial(u) }
	}
	conn, err := dial(ctx, c.config.URL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	tempCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := c.setupExchanges(tempCh); err != nil {
		tempCh.Close()
		conn.Close()
		return fmt.Errorf("declare exchanges: %w", err)
	}
	tempCh.Close()

	size := c.config.PublishPoolSize
	if size <= 0 {
		size = 16
	}
	pool, err := NewChannelPool(conn, size)
	if err != nil {
		conn.Close()
		return fmt.Errorf("new pool: %w", err)
	}

	c.conn = conn
	c.pool = pool
	if c.logger != nil {
		c.logger.With("op", op).Info("reconnected")
	}
	return nil
}
