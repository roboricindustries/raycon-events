//go:build integration

package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roboricindustries/raycon-events/pkg/pubsub/v2/chanpool"
)

func TestChanPool_A3_CloseUnblocksBlockedLease(t *testing.T) {
	conn, err := amqp.Dial(ensureRabbit(t))
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	pool, err := chanpool.New(conn, 1)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	first, err := pool.Lease(context.Background())
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	defer first.Release()

	errCh := make(chan error, 1)
	go func() {
		_, leaseErr := pool.Lease(context.Background())
		errCh <- leaseErr
	}()

	time.Sleep(150 * time.Millisecond)
	pool.Close()

	select {
	case leaseErr := <-errCh:
		if !errors.Is(leaseErr, chanpool.ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", leaseErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("blocked lease did not unblock on pool close")
	}
}

func TestChanPool_A4_WarmupFailsFastOnConfigureError(t *testing.T) {
	conn, err := amqp.Dial(ensureRabbit(t))
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	_, err = chanpool.New(
		conn,
		2,
		chanpool.WithWarmup(),
		chanpool.WithConfigure(func(_ *amqp.Channel) error {
			return errors.New("warmup configure failure")
		}),
	)
	if err == nil {
		t.Fatalf("expected warmup creation to fail")
	}
}
