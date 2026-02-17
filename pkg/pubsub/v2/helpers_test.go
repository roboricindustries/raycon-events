package pubsub

import (
	"math"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDeathCount_MatchingQueue(t *testing.T) {
	d := amqp.Delivery{
		Headers: amqp.Table{
			"x-death": []any{
				amqp.Table{
					"queue": "other.queue",
					"count": int64(7),
				},
				amqp.Table{
					"queue": "target.queue",
					"count": int64(3),
				},
			},
		},
	}

	if got := DeathCount(d, "target.queue"); got != 3 {
		t.Fatalf("expected death count 3, got %d", got)
	}
}

func TestDeathCount_MalformedHeaders(t *testing.T) {
	cases := []amqp.Delivery{
		{Headers: amqp.Table{}},
		{Headers: amqp.Table{"x-death": "bad-type"}},
		{Headers: amqp.Table{"x-death": []any{"bad-entry"}}},
		{Headers: amqp.Table{"x-death": []any{amqp.Table{"queue": "target.queue", "count": "bad"}}}},
	}

	for i, d := range cases {
		if got := DeathCount(d, "target.queue"); got != 0 {
			t.Fatalf("case %d: expected 0, got %d", i, got)
		}
	}
}

func TestTTLMillis_Overflow(t *testing.T) {
	tooLarge := (time.Duration(math.MaxInt32) + 1) * time.Millisecond
	if _, err := ttlMillis(tooLarge); err == nil {
		t.Fatalf("expected overflow error")
	}
}

func TestTTLMillis_Negative(t *testing.T) {
	if _, err := ttlMillis(-1 * time.Millisecond); err == nil {
		t.Fatalf("expected negative ttl error")
	}
}

func TestTTLMillis_Valid(t *testing.T) {
	got, err := ttlMillis(250 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 250 {
		t.Fatalf("expected 250, got %d", got)
	}
}
