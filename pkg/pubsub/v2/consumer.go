package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roboricindustries/raycon-events/pkg/pubsub/v2/chanpool"
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

// DispatchSpec enables keyed-lane dispatch for a consumer:
// ordered processing within a key, concurrent processing across keys.
//
// Non-breaking: if Dispatch is nil or Lanes <= 1, consumers run in legacy synchronous mode.
type DispatchSpec struct {
	// Lanes is the number of processing lanes (shards). Must be >= 2 to enable dispatch.
	Lanes int

	// MaxInFlight bounds the number of deliveries that are "in processing" (queued to lanes or executing handler)
	// and therefore unacked. If <= 0, defaults to the effective Prefetch.
	MaxInFlight int

	// LaneQueueSize bounds the buffered queue per lane.
	// If <= 0, a safe default derived from MaxInFlight/Lanes is used.
	LaneQueueSize int

	// KeyFunc extracts a deterministic key used for lane selection.
	// All deliveries with the same key are processed sequentially in the same lane.
	KeyFunc func(d amqp.Delivery) (string, error)
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

	Dispatch *DispatchSpec // nil => legacy synchronous behavior

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
	c.consumerClosed = make(chan consumerClosedEvent, len(specs)*4)
	c.consumerSpecs = make(map[string]ConsumerSpec, len(specs))

	for _, s := range specs {
		c.consumerSpecs[s.Name] = s
		if err := c.startConsumer(ctx, s); err != nil {
			return fmt.Errorf("start %s: %w", s.Name, err)
		}
	}

	conn, _, _, _ := c.snapshot()
	if conn == nil {
		return fmt.Errorf("nil connection")
	}
	errCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	base := Dsec(c.config.ReconnectBackoffBaseSeconds, 1)
	capd := Dsec(c.config.ReconnectBackoffCapSeconds, 30)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev := <-c.consumerClosed:
			// Ignore stale events from older connections.
			if ev.gen != c.currentGen() {
				continue
			}
			if s, ok := c.consumerSpecs[ev.name]; ok {
				if err := c.startConsumer(ctx, s); err != nil {
					if c.logger != nil {
						c.logger.Error("restart consumer failed", slog.String("name", ev.name), slog.Any("error", err))
					}
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
					t := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						t.Stop()
						return ctx.Err()
					case <-t.C:
					}
					if backoff*2 < capd {
						backoff *= 2
					}
					continue
				}

				// success → restart all consumers on new conn
				for _, s := range c.consumerSpecs {
					if err := c.startConsumer(ctx, s); err != nil {
						if c.logger != nil {
							c.logger.Error("restart consumer after reconnect failed", slog.String("name", s.Name), slog.Any("error", err))
						}
					}
				}

				conn, _, _, _ := c.snapshot()
				if conn == nil {
					return fmt.Errorf("nil connection after reconnect")
				}
				errCh = conn.NotifyClose(make(chan *amqp.Error, 1))
				break
			}
		}
	}
}

// startConsumer declares the per-consumer topology and runs the loop.
func (c *Client) startConsumer(ctx context.Context, spec ConsumerSpec) error {
	conn, _, gen, logger := c.snapshot()
	if conn == nil {
		return fmt.Errorf("nil connection")
	}

	ch, err := conn.Channel()
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

		// Optional keyed-lane dispatch mode (non-breaking): enabled only when configured.
		if spec.Dispatch != nil && spec.Dispatch.Lanes > 1 && spec.Dispatch.KeyFunc != nil {
			c.dispatchConsumeLoop(ctx, ch, spec, pf, msgs, closeCh, gen, logger)
			return
		}

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
				c.notifyConsumerClosed(ctx, spec.Name, gen)
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
						finalEx := FirstNonEmpty(spec.Retry.FinalExchange, spec.Queue+".final")

						if err := PublishFinal(ch, finalEx, d); err != nil {
							if logger != nil {
								logger.Error("publish final failed; sending back to retry (DLX)",
									slog.Any("error", err),
									slog.String("queue", spec.Queue))
							}
							// Use DLX backoff instead of immediate requeue.
							_ = d.Nack(false, false)
							continue
						}

						_ = d.Ack(false)
						continue
					}
				}

				err := safeConsume(ctx, spec, d, logger)
				switch {
				case errors.Is(err, ErrPoison):
					if spec.PoisonToFinal {
						finalEx := FirstNonEmpty(TryFinalEx(spec), spec.Queue+".final")
						if err := PublishFinal(ch, finalEx, d); err != nil {
							if logger != nil {
								logger.Error("publish final failed for poison; requeueing",
									slog.Any("error", err), slog.String("queue", spec.Queue))
							}
							// If retry enabled, prefer DLX (backoff); else requeue once.
							if spec.Retry != nil && spec.Retry.Enabled {
								_ = d.Nack(false, false)
							} else {
								_ = d.Nack(false, true)
							}
							continue // <- DO NOT return
						}
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

	if logger != nil {
		logger.Info("consumer started", slog.String("name", spec.Name), slog.String("queue", spec.Queue), slog.Int("prefetch", pf))
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
//
// Concurrency note: conn/pool swaps are guarded; old instances are closed after the swap.
func (c *Client) reconnect(ctx context.Context) error {
	const op = "rabbitmq.reconnect"

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
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := c.setupExchanges(tempCh); err != nil {
		_ = tempCh.Close()
		_ = conn.Close()
		return fmt.Errorf("declare exchanges: %w", err)
	}
	_ = tempCh.Close()

	size := c.config.PublishPoolSize
	if size <= 0 {
		size = 16
	}
	pool, err := chanpool.New(conn, size)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("new pool: %w", err)
	}

	// Swap under lock, then close old.
	c.mu.Lock()
	oldConn := c.conn
	oldPool := c.pool
	c.conn = conn
	c.pool = pool
	c.gen++
	newGen := c.gen
	c.mu.Unlock()

	if oldPool != nil {
		oldPool.Close()
	}
	if oldConn != nil && !oldConn.IsClosed() {
		_ = oldConn.Close()
	}

	if c.logger != nil {
		c.logger.With("op", op).Info("reconnected", slog.Uint64("gen", newGen))
	}
	return nil
}
