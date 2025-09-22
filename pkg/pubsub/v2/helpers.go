package pubsub

import (
	"context"
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

func PublishFinal(ch *amqp.Channel, exchange string, d amqp.Delivery) error {
	return ch.PublishWithContext(context.Background(), exchange, "", false, false, amqp.Publishing{
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

func SafeClose(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}
	defer func() { _ = recover() }()
	return ch.Close()
}
