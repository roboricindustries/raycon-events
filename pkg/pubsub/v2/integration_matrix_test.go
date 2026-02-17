//go:build integration

package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPubSub_A1_ConcurrentPublishDeliversAll(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 8, 1)
	_, ch := openTestChannel(t)

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	const total = 100
	var wg sync.WaitGroup
	errCh := make(chan error, total)

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- publishEnvelope(t, client, exchange, routingKey, strconv.Itoa(i))
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("publish failed: %v", err)
		}
	}

	waitUntilQueueDepth(t, ch, queue, total, 10*time.Second)
}

func TestPubSub_A2_PublishErrorDoesNotPoisonPool(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	badExchange := uniqueName(t, "bad_ex")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	// Broker can close the channel asynchronously on bad exchange publish.
	// So this call may return an error immediately, or appear successful and fail shortly after.
	_ = publishEnvelope(t, client, badExchange, routingKey, "bad")

	eventually(t, 8*time.Second, 100*time.Millisecond, func() bool {
		_ = publishEnvelope(t, client, exchange, routingKey, fmt.Sprintf("good-%d", time.Now().UnixNano()))
		n, err := queueDepth(ch, queue)
		return err == nil && n >= 1
	}, "at least one good message should be routed")
}

func TestPubSub_B1_ConsumerAcksAndDrainsQueue(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 8)
	_, ch := openTestChannel(t)

	var processed atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for i := 0; i < 10; i++ {
		publishRaw(t, ch, exchange, routingKey, []byte(fmt.Sprintf(`{"n":%d}`, i)), nil)
	}

	eventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 10
	}, "all messages should be consumed")
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_B3_PanicRecoveredAndConsumerContinues(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")
	finalExchange := uniqueName(t, "final_ex")
	finalQueue := uniqueName(t, "final_q")

	client := newTestClient(t, exchange, routingKey, 4, 2)
	_, ch := openTestChannel(t)

	var successCount atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:       true,
			TTL:           80 * time.Millisecond,
			MaxAttempts:   1,
			DeadExchange:  deadExchange,
			DeadQueue:     deadQueue,
			FinalExchange: finalExchange,
			FinalQueue:    finalQueue,
		},
		Consume: func(_ context.Context, d amqp.Delivery) error {
			if bytes.Contains(d.Body, []byte(`"panic":true`)) {
				panic("panic test")
			}
			successCount.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"panic":true}`), nil)
	publishRaw(t, ch, exchange, routingKey, []byte(`{"ok":true}`), nil)

	eventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		return successCount.Load() == 1
	}, "consumer should process good message after panic")
	waitUntilQueueDepth(t, ch, finalQueue, 1, 8*time.Second)
}

func TestPubSub_C1_RetryThenSuccessAfterTTL(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")

	client := newTestClient(t, exchange, routingKey, 4, 2)
	_, ch := openTestChannel(t)

	var attempts atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:      true,
			TTL:          100 * time.Millisecond,
			MaxAttempts:  5,
			DeadExchange: deadExchange,
			DeadQueue:    deadQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			n := attempts.Add(1)
			if n <= 2 {
				return errors.New("transient")
			}
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	start := time.Now()
	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"c1"}`), nil)

	eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
		return attempts.Load() == 3
	}, "message should succeed on third attempt")

	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Fatalf("retry cycle was too fast, elapsed=%v", elapsed)
	}
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_C2_MaxAttemptsRoutesToFinal(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	deadExchange := uniqueName(t, "dead_ex")
	deadQueue := uniqueName(t, "dead_q")
	finalExchange := uniqueName(t, "final_ex")
	finalQueue := uniqueName(t, "final_q")

	client := newTestClient(t, exchange, routingKey, 4, 4)
	_, ch := openTestChannel(t)

	var attempts atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Retry: &RetrySpec{
			Enabled:       true,
			TTL:           120 * time.Millisecond,
			MaxAttempts:   2,
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
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"c2"}`), nil)
	waitUntilQueueDepth(t, ch, finalQueue, 1, 12*time.Second)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 handler attempts, got %d", got)
	}
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_D1_PoisonToFinalPublishesAndAcks(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	finalExchange := uniqueName(t, "final_ex")
	finalQueue := uniqueName(t, "final_q")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	spec := ConsumerSpec{
		Name:          uniqueName(t, "consumer"),
		Exchange:      exchange,
		Queue:         queue,
		BindingKey:    routingKey,
		PoisonToFinal: true,
		Retry: &RetrySpec{
			FinalExchange: finalExchange,
			FinalQueue:    finalQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			return ErrPoison
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"d1"}`), nil)
	waitUntilQueueDepth(t, ch, finalQueue, 1, 5*time.Second)
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)
}

func TestPubSub_D2_PoisonWithoutFinalOnlyAcks(t *testing.T) {
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
			return ErrPoison
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"d2"}`), nil)

	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return attempts.Load() == 1
	}, "poison should be handled once")
	waitUntilQueueDepth(t, ch, queue, 0, 5*time.Second)

	if _, err := ch.QueueInspect(queue + ".final"); err == nil {
		t.Fatalf("final queue should not exist when poison_to_final=false and retry disabled")
	}
}

func TestPubSub_D3_PublishFinalFailureDoesNotAckPoison(t *testing.T) {
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
		Name:          uniqueName(t, "consumer"),
		Exchange:      exchange,
		Queue:         queue,
		BindingKey:    routingKey,
		PoisonToFinal: true,
		Retry: &RetrySpec{
			Enabled:       true,
			TTL:           80 * time.Millisecond,
			MaxAttempts:   10,
			DeadExchange:  deadExchange,
			DeadQueue:     deadQueue,
			FinalExchange: finalExchange,
			FinalQueue:    finalQueue,
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			attempts.Add(1)
			return ErrPoison
		},
	}

	cancel, done := runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	if err := ch.ExchangeDelete(finalExchange, false, false); err != nil {
		t.Fatalf("delete final exchange: %v", err)
	}

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"d3"}`), nil)

	eventually(t, 10*time.Second, 100*time.Millisecond, func() bool {
		return attempts.Load() >= 2
	}, "poison message should be retried when final publish fails")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("run loop did not exit after cancel")
	}

	eventually(t, 5*time.Second, 100*time.Millisecond, func() bool {
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
		return mainDepth+retryDepth+finalDepth >= 1
	}, "message should not be lost when final publish fails")
}

func TestPubSub_E1_DispatchOrdersWithinKeyAndRunsAcrossKeys(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 20)
	_, ch := openTestChannel(t)

	type payload struct {
		Seq int `json:"seq"`
	}

	var (
		mu              sync.Mutex
		order           = map[string][]int{"A": {}, "B": {}}
		activePerKey    = map[string]int{"A": 0, "B": 0}
		globalInFlight  int
		maxInFlight     int
		sameKeyParallel bool
		processed       atomic.Int32
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
				key, ok := d.Headers["conv_key"].(string)
				if !ok || key == "" {
					return "", fmt.Errorf("missing conv_key")
				}
				return key, nil
			},
		},
		Consume: func(_ context.Context, d amqp.Delivery) error {
			key, ok := d.Headers["conv_key"].(string)
			if !ok || key == "" {
				return ErrPoison
			}

			var p payload
			if err := json.Unmarshal(d.Body, &p); err != nil {
				return ErrPoison
			}

			mu.Lock()
			globalInFlight++
			if globalInFlight > maxInFlight {
				maxInFlight = globalInFlight
			}
			activePerKey[key]++
			if activePerKey[key] > 1 {
				sameKeyParallel = true
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)

			mu.Lock()
			order[key] = append(order[key], p.Seq)
			activePerKey[key]--
			globalInFlight--
			mu.Unlock()

			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	for _, msg := range []struct {
		key string
		seq int
	}{
		{key: "A", seq: 1},
		{key: "B", seq: 1},
		{key: "A", seq: 2},
		{key: "B", seq: 2},
		{key: "A", seq: 3},
		{key: "B", seq: 3},
	} {
		body, err := json.Marshal(payload{Seq: msg.seq})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		publishRaw(t, ch, exchange, routingKey, body, amqp.Table{"conv_key": msg.key})
	}

	eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 6
	}, "all dispatch messages should be processed")

	mu.Lock()
	defer mu.Unlock()

	if !reflect.DeepEqual(order["A"], []int{1, 2, 3}) {
		t.Fatalf("key A order mismatch: %v", order["A"])
	}
	if !reflect.DeepEqual(order["B"], []int{1, 2, 3}) {
		t.Fatalf("key B order mismatch: %v", order["B"])
	}
	if sameKeyParallel {
		t.Fatalf("same key was processed in parallel")
	}
	if maxInFlight < 2 {
		t.Fatalf("expected cross-key concurrency, max in-flight=%d", maxInFlight)
	}
}

func TestPubSub_E5_KeyFuncErrorFollowsPoisonPolicy(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")
	finalExchange := uniqueName(t, "final_ex")
	finalQueue := uniqueName(t, "final_q")

	client := newTestClient(t, exchange, routingKey, 4, 5)
	_, ch := openTestChannel(t)

	var consumed atomic.Int32
	spec := ConsumerSpec{
		Name:          uniqueName(t, "consumer"),
		Exchange:      exchange,
		Queue:         queue,
		BindingKey:    routingKey,
		PoisonToFinal: true,
		Retry: &RetrySpec{
			FinalExchange: finalExchange,
			FinalQueue:    finalQueue,
		},
		Dispatch: &DispatchSpec{
			Lanes:       2,
			MaxInFlight: 5,
			KeyFunc: func(d amqp.Delivery) (string, error) {
				if bad, _ := d.Headers["bad_key"].(bool); bad {
					return "", fmt.Errorf("bad key")
				}
				key, ok := d.Headers["conv_key"].(string)
				if !ok || key == "" {
					return "", fmt.Errorf("missing conv_key")
				}
				return key, nil
			},
		},
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			consumed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)
	waitUntilQueueExists(t, ch, finalQueue, 5*time.Second)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"case":"e5"}`), amqp.Table{
		"bad_key":  true,
		"conv_key": "A",
	})

	waitUntilQueueDepth(t, ch, finalQueue, 1, 5*time.Second)
	if got := consumed.Load(); got != 0 {
		t.Fatalf("handler should not run when keyfunc fails, got %d calls", got)
	}
}

func TestPubSub_F2_ReconnectResumesConsumption(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 2)
	_, ch := openTestChannel(t)

	var processed atomic.Int32
	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	if err := publishEnvelope(t, client, exchange, routingKey, "before-reconnect"); err != nil {
		t.Fatalf("initial publish failed: %v", err)
	}
	eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 1
	}, "first message should be consumed")

	client.mu.RLock()
	conn := client.conn
	client.mu.RUnlock()
	if conn == nil {
		t.Fatalf("client connection is nil")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}

	eventually(t, 15*time.Second, 200*time.Millisecond, func() bool {
		return publishEnvelope(t, client, exchange, routingKey, "after-reconnect") == nil
	}, "publish should succeed again after reconnect")

	eventually(t, 10*time.Second, 100*time.Millisecond, func() bool {
		return processed.Load() >= 2
	}, "consumer should resume after reconnect")

	time.Sleep(500 * time.Millisecond)
	if got := processed.Load(); got != 2 {
		t.Fatalf("expected exactly 2 processed messages, got %d", got)
	}
}

func TestPubSub_G1_StaleConsumerClosedIgnored(t *testing.T) {
	exchange := uniqueName(t, "ex")
	queue := uniqueName(t, "q")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 4, 1)
	_, ch := openTestChannel(t)

	var (
		processed atomic.Int32
		active    atomic.Int32
		maxActive atomic.Int32
	)

	spec := ConsumerSpec{
		Name:       uniqueName(t, "consumer"),
		Exchange:   exchange,
		Queue:      queue,
		BindingKey: routingKey,
		Prefetch:   1,
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			cur := active.Add(1)
			for {
				prev := maxActive.Load()
				if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(120 * time.Millisecond)
			active.Add(-1)
			processed.Add(1)
			return nil
		},
	}

	runConsumers(t, client, spec)
	waitUntilQueueExists(t, ch, queue, 5*time.Second)

	staleGen := uint64(0)
	if g := client.currentGen(); g > 0 {
		staleGen = g - 1
	}
	client.notifyConsumerClosed(context.Background(), spec.Name, staleGen)

	publishRaw(t, ch, exchange, routingKey, []byte(`{"idx":1}`), nil)
	publishRaw(t, ch, exchange, routingKey, []byte(`{"idx":2}`), nil)

	eventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		return processed.Load() == 2
	}, "messages should be consumed sequentially")

	if got := maxActive.Load(); got > 1 {
		t.Fatalf("stale consumerClosed was not ignored; max concurrent handlers=%d", got)
	}
}

func TestPubSub_G3_DuplicateConsumerNameRejected(t *testing.T) {
	exchange := uniqueName(t, "ex")
	routingKey := uniqueName(t, "rk")

	client := newTestClient(t, exchange, routingKey, 2, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	specA := ConsumerSpec{
		Name:       "dup-name",
		Exchange:   exchange,
		Queue:      uniqueName(t, "q"),
		BindingKey: routingKey,
		Consume: func(_ context.Context, _ amqp.Delivery) error {
			return nil
		},
	}
	specB := specA
	specB.Queue = uniqueName(t, "q")

	err := client.RunWithConsumers(ctx, specA, specB)
	if err == nil {
		t.Fatalf("expected duplicate consumer name error")
	}
	if !strings.Contains(err.Error(), "duplicate consumer name") {
		t.Fatalf("unexpected error: %v", err)
	}
}
