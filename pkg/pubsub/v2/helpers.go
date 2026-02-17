package pubsub

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Dsec(v, def int) time.Duration {
	if v <= 0 {
		return time.Duration(def) * time.Second
	}
	return time.Duration(v) * time.Second
}

func JitteredDelay(base, cap time.Duration, jitterPct int) time.Duration {
	if jitterPct <= 0 {
		jitterPct = 25
	}
	delta := (rand.Float64()*2 - 1) * float64(jitterPct) / 100.0
	wait := time.Duration(float64(base) * (1 + delta))
	if wait < 0 {
		wait = base
	}
	if wait > cap {
		wait = cap
	}
	return wait
}

func FirstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func TryFinalEx(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.FinalExchange != "" {
		return s.Retry.FinalExchange
	}
	return ""
}

func TryFinalQ(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.FinalQueue != "" {
		return s.Retry.FinalQueue
	}
	return ""
}

func DeathCount(d amqp.Delivery, queue string) int {
	raw, ok := d.Headers["x-death"]
	if !ok {
		return 0
	}
	list, ok := raw.([]any)
	if !ok {
		return 0
	}
	for _, it := range list {
		if m, ok := it.(amqp.Table); ok {
			if q, _ := m["queue"].(string); q == queue {
				if n, ok := m["count"].(int64); ok {
					return int(n)
				}
			}
		}
	}
	return 0
}

func ttlMillis(ttl time.Duration) (int32, error) {
	ms := ttl / time.Millisecond
	if ms < 0 {
		return 0, fmt.Errorf("ttl must be >= 0, got %s", ttl)
	}
	if ms > math.MaxInt32 {
		return 0, fmt.Errorf("ttl is too large: %s", ttl)
	}
	return int32(ms), nil
}

// PublishFinalWithContext publishes a copy of a delivery to a final (fanout) exchange.
func PublishFinalWithContext(ctx context.Context, ch *amqp.Channel, exchange string, d amqp.Delivery) error {
	return ch.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{
		ContentType:   FirstNonEmpty(d.ContentType, "application/json"),
		Body:          d.Body,
		Headers:       d.Headers,
		MessageId:     d.MessageId,
		CorrelationId: d.CorrelationId,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
		Type:          d.Type,
		AppId:         d.AppId,
	})
}

// PublishFinal is a compatibility wrapper that uses a bounded timeout.
// This prevents the ACK manager goroutine from hanging indefinitely during broker issues.
func PublishFinal(ch *amqp.Channel, exchange string, d amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return PublishFinalWithContext(ctx, ch, exchange, d)
}

func SafeClose(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}
	defer func() { _ = recover() }()
	return ch.Close()
}

func deadEx(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.DeadExchange != "" {
		return s.Retry.DeadExchange
	}
	return s.Queue + ".dead"
}

func deadQ(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.DeadQueue != "" {
		return s.Retry.DeadQueue
	}
	return s.Queue + ".dead"
}

func finalEx(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.FinalExchange != "" {
		return s.Retry.FinalExchange
	}
	return s.Queue + ".final"
}

func finalQ(s ConsumerSpec) string {
	if s.Retry != nil && s.Retry.FinalQueue != "" {
		return s.Retry.FinalQueue
	}
	return s.Queue + ".final"
}
