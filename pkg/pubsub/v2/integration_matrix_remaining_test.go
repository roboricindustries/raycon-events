//go:build integration

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPubSub_B2_AckAfterHandlerReturn(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:      true,
			TTL:          5 * time.Second,
			MaxAttempts:  10,
			DeadExchange: deadExchange,
			DeadQueue:    deadQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-unblock
			return errors.New("fail after unblock")
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, deadQueue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"b2"}`), nil)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not start")
	}

	if depth, err := queueDepth(ch, deadQueue); err != nil || depth != 0 {
		t.Fatalf("dead queue should stay empty while handler is blocked, depth=%d err=%v", depth, err)
	}

	close(unblock)
	waitUntilQueueDepth(t, ch, deadQueue, 1, 5*time.Second)
}

func TestPubSub_C3_DeathCountIncrementsAcrossRetries(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	var (
		mu     sync.Mutex
		counts []int
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:      true,
			TTL:          80 * time.Millisecond,
			MaxAttempts:  10,
			DeadExchange: deadExchange,
			DeadQueue:    deadQueue,
		},
		Consume: func(_ context.Context, d amqp.Delivery) error {
			dc := DeathCount(d, queue)
			mu.Lock()
			counts = append(counts, dc)
			mu.Unlock()
			if dc < 2 {
				return errors.New("retry")
			}
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"c3"}`), nil)

	eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) >= 3
	}, "expected 3 deliveries")

	mu.Lock()
	got := append([]int(nil), counts...)
	mu.Unlock()

	if len(got) < 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("unexpected death counts: %v", got)
	}
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_C4_SmallTTLDelayApplied(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	var (
		firstAttempt  atomic.Int64
		secondAttempt atomic.Int64
		attempts      atomic.Int32
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:      true,
			TTL:          60 * time.Millisecond,
			MaxAttempts:  3,
			DeadExchange: deadExchange,
			DeadQueue:    deadQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			n := attempts.Add(1)
			now := time.Now().UnixNano()
			if n == 1 {
				firstAttempt.Store(now)
				return errors.New("first fail")
			}
			if n == 2 {
				secondAttempt.Store(now)
			}
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"c4"}`), nil)

	eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
		return attempts.Load() >= 2
	}, "expected second attempt")

	a := firstAttempt.Load()
	b := secondAttempt.Load()
	if a == 0 || b == 0 {
		t.Fatalf("attempt timestamps missing: a=%d b=%d", a, b)
	}
	if delta := time.Duration(b - a); delta < 40*time.Millisecond {
		t.Fatalf("ttl delay was too short: %v", delta)
	}
}

func TestPubSub_D4_FinalPublishFailureStopsAfterExtraBudget(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")
	finalExchange := uniqueName(t, "final_ex")
	finalQueue := uniqueName(t, "final_q")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	var attempts atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:       true,
			TTL:           70 * time.Millisecond,
			MaxAttempts:   1,
			DeadExchange:  deadExchange,
			DeadQueue:     deadQueue,
			FinalExchange: finalExchange,
			FinalQueue:    finalQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			attempts.Add(1)
			return errors.New("always fail")
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, deadQueue, 5*time.Second)
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	if err := ch.ExchangeDelete(finalExchange, false, false); err != nil {
		t.Fatalf("delete final exchange: %v", err)
	}

	startedAt := time.Now()
	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"d4"}`), nil)

	eventually(t, 20*time.Second, 100*time.Millisecond, func() bool {
		mainDepth, err := queueDepth(ch, queue)
		if err != nil {
			return false
		}
		retryDepth, err := queueDepth(ch, deadQueue)
		if err != nil {
			return false
		}
		finalDepth, err := queueDepth(ch, finalQueue)
		if err != nil {
			return false
		}
		// Terminal states:
		// 1) dropped after extra budget (finalDepth == 0), or
		// 2) eventually routed to final queue after reconnect/redeclare (finalDepth >= 1).
		// In both cases, main/dead queues must drain to 0 (no infinite loop).
		return mainDepth == 0 && retryDepth == 0 && finalDepth >= 0
	}, "message should stop looping after final publish instability")

	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected handler attempts=1 (max-attempt gate), got %d", got)
	}

	// Ensure the stable condition persists (not a transient drain before another loop).
	time.Sleep(300 * time.Millisecond)
	mainDepth, err := queueDepth(ch, queue)
	if err != nil {
		t.Fatalf("inspect main queue: %v", err)
	}
	retryDepth, err := queueDepth(ch, deadQueue)
	if err != nil {
		t.Fatalf("inspect dead queue: %v", err)
	}
	if mainDepth != 0 || retryDepth != 0 {
		t.Fatalf("loop did not stabilize, main=%d dead=%d elapsed=%v", mainDepth, retryDepth, time.Since(startedAt))
	}
}

func TestPubSub_E2_SameKeyInferenceNoParallel(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 8)
	_, ch := openTestChannel(t)

	var (
		active    atomic.Int32
		maxActive atomic.Int32
		processed atomic.Int32
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Dispatch: &DispatchSpec{
			Lanes:       4,
			MaxInFlight: 8,
			KeyFunc: func(d amqp.Delivery) (string, error) {
				return d.Headers["conv_key"].(string), nil
			},
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			cur := active.Add(1)
			for {
				prev := maxActive.Load()
				if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			active.Add(-1)
			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for i := 0; i < 10; i++ {
		publishRaw(t, ch, exchange, routingKey, []byte(fmt.Sprintf(`{"n":%d}`, i)), amqp.Table{"conv_key": "A"})
	}

	eventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 10
	}, "all same-key messages should complete")

	if got := maxActive.Load(); got > 1 {
		t.Fatalf("same-key parallelism detected: max=%d", got)
	}
}

func TestPubSub_E3_NoParallelismForSameKey(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 20)
	_, ch := openTestChannel(t)

	var (
		mu       sync.Mutex
		order    []int
		parallel atomic.Bool
		active   atomic.Int32
		done     atomic.Int32
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Dispatch: &DispatchSpec{
			Lanes:       4,
			MaxInFlight: 20,
			KeyFunc: func(d amqp.Delivery) (string, error) {
				return d.Headers["conv_key"].(string), nil
			},
		},
		Consume: func(_ context.Context, d amqp.Delivery) error {
			cur := active.Add(1)
			if cur > 1 {
				parallel.Store(true)
			}
			var seq int
			_, _ = fmt.Sscanf(string(d.Body), `{"seq":%d}`, &seq)
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			order = append(order, seq)
			mu.Unlock()
			active.Add(-1)
			done.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for i := 1; i <= 100; i++ {
		publishRaw(t, ch, exchange, routingKey, []byte(fmt.Sprintf(`{"seq":%d}`, i)), amqp.Table{"conv_key": "A"})
	}

	eventually(t, 15*time.Second, 50*time.Millisecond, func() bool {
		return done.Load() == 100
	}, "all same-key messages should finish")

	if parallel.Load() {
		t.Fatalf("same key processed in parallel")
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i <= 100; i++ {
		if order[i-1] != i {
			t.Fatalf("order mismatch at %d: %v", i, order[:minInt(len(order), 10)])
		}
	}
}

func TestPubSub_E4_LaneBackpressureNoDeadlock(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 2)
	_, ch := openTestChannel(t)

	unblock := make(chan struct{})
	var (
		started   atomic.Int32
		processed atomic.Int32
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Prefetch:   2,
		Dispatch: &DispatchSpec{
			Lanes:         2,
			MaxInFlight:   2,
			LaneQueueSize: 1,
			KeyFunc: func(d amqp.Delivery) (string, error) {
				return d.Headers["conv_key"].(string), nil
			},
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			started.Add(1)
			<-unblock
			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for i := 0; i < 10; i++ {
		key := "A"
		if i%2 == 0 {
			key = "B"
		}
		publishRaw(t, ch, exchange, routingKey, []byte(fmt.Sprintf(`{"n":%d}`, i)), amqp.Table{"conv_key": key})
	}

	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return started.Load() >= 2
	}, "first two messages should start")

	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		depth, err := queueDepth(ch, queue)
		return err == nil && depth >= 6
	}, "backpressure should keep messages queued while handlers block")

	close(unblock)

	eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 10
	}, "dispatch should continue after unblocking")
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_F1_GracefulShutdownDrainsAndRedelivers(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 2)
	_, ch := openTestChannel(t)

	var (
		firstCompleted atomic.Int32
		replayed       atomic.Int32
		mu             sync.Mutex
		seenFirst      = map[int]struct{}{}
		seenSecond     = map[int]struct{}{}
	)
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Prefetch:   2,
		Dispatch: &DispatchSpec{
			Lanes:       2,
			MaxInFlight: 2,
			KeyFunc: func(d amqp.Delivery) (string, error) {
				return d.Headers["conv_key"].(string), nil
			},
		},
		Consume: func(_ context.Context, d amqp.Delivery) error {
			var n int
			_, _ = fmt.Sscanf(string(d.Body), `{"n":%d}`, &n)
			mu.Lock()
			seenFirst[n] = struct{}{}
			mu.Unlock()

			time.Sleep(250 * time.Millisecond)
			firstCompleted.Add(1)
			return nil
		},
	}

	cancel, done := runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for i := 0; i < 6; i++ {
		publishRaw(t, ch, exchange, routingKey, []byte(fmt.Sprintf(`{"n":%d}`, i)), amqp.Table{"conv_key": "K"})
	}

	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return firstCompleted.Load() >= 1
	}, "at least one message should complete before cancel")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("consumer loop did not stop gracefully")
	}

	completedBeforeStop := firstCompleted.Load()
	if completedBeforeStop <= 0 || completedBeforeStop >= 6 {
		t.Fatalf("expected partial completion before restart, got %d", completedBeforeStop)
	}

	secondClient := newTestClient(t, exchange, routingKey, 4, 2)
	replaySpec := ConsumerSpec{
		Name:       uniqueName(t, "consumer2"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Consume: func(_ context.Context, d amqp.Delivery) error {
			var n int
			_, _ = fmt.Sscanf(string(d.Body), `{"n":%d}`, &n)
			mu.Lock()
			seenSecond[n] = struct{}{}
			mu.Unlock()

			replayed.Add(1)
			return nil
		},
	}
	runConsumers(t, secondClient, replaySpec)

	eventually(t, 12*time.Second, 50*time.Millisecond, func() bool {
		depth, err := queueDepth(ch, queue)
		if err != nil || depth != 0 {
			return false
		}
		if replayed.Load() == 0 {
			return false
		}

		mu.Lock()
		defer mu.Unlock()
		seen := map[int]struct{}{}
		for n := range seenFirst {
			seen[n] = struct{}{}
		}
		for n := range seenSecond {
			seen[n] = struct{}{}
		}
		return len(seen) == 6
	}, "remaining messages should be redelivered after restart")
}

func TestPubSub_G2_AllConsumersRecoverUnderConnectionFlap(t *testing.T) {
	exchange := uniqueName(t, "ex")
	rks := []string{
		uniqueName(t, "rk1"),
		uniqueName(t, "rk2"),
		uniqueName(t, "rk3"),
	}
	qs := []string{
		uniqueName(t, "q1"),
		uniqueName(t, "q2"),
		uniqueName(t, "q3"),
	}

	client := newTestClient(t, exchange, rks[0], 4, 1)
	_, ch := openTestChannel(t)

	var counters [3]atomic.Int32
	specs := make([]ConsumerSpec, 0, 3)
	for i := 0; i < 3; i++ {
		idx := i
		specs = append(specs, ConsumerSpec{
			Name:       uniqueName(t, fmt.Sprintf("consumer_%d", i)),
			Exchange:   exchange,
			Queue:      qs[i],
			BindingKey: rks[i],
			Consume: func(_ context.Context, _ amqp.Delivery) error {
				counters[idx].Add(1)
				return nil
			},
		})
	}

	runConsumers(t, client, specs...)
	for _, q := range qs {
		waitUntilQueueExists(t, ch, q, 5*time.Second)
	}

	for i := 0; i < 3; i++ {
		publishRaw(t, ch, exchange, rks[i], []byte(fmt.Sprintf(`{"phase":"baseline","idx":%d}`, i)), nil)
	}
	eventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		for i := 0; i < 3; i++ {
			if counters[i].Load() < 1 {
				return false
			}
		}
		return true
	}, "baseline messages should be consumed by all specs")

	for i := 0; i < 3; i++ {
		client.mu.RLock()
		conn := client.conn
		client.mu.RUnlock()
		if conn != nil {
			_ = conn.Close()
		}
		time.Sleep(1200 * time.Millisecond)
	}

	for i := 0; i < 3; i++ {
		idx := i
		eventually(t, 15*time.Second, 200*time.Millisecond, func() bool {
			err := publishEnvelope(t, client, exchange, rks[idx], fmt.Sprintf("g2-%d", idx))
			return err == nil
		}, fmt.Sprintf("publish after flap for consumer %d", i))
	}

	eventually(t, 12*time.Second, 100*time.Millisecond, func() bool {
		for i := 0; i < 3; i++ {
			if counters[i].Load() < 2 {
				return false
			}
		}
		return true
	}, "all consumers should recover after flaps")
}

func TestPubSub_H1_RetryDisabledDoesNotHotLoop(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	var attempts atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			attempts.Add(1)
			return errors.New("deterministic failure")
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"h1"}`), nil)

	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return attempts.Load() >= 2
	}, "message should be redelivered once")

	time.Sleep(500 * time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected exactly 2 attempts with hot-loop guard, got %d", got)
	}
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
