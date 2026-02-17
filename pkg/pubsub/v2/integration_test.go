//go:build integration

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	itOnce      sync.Once
	itContainer testcontainers.Container
	itAMQPURL   string
	itInitErr   error
	itNameSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if itContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = itContainer.Terminate(ctx)
		cancel()
	}
	os.Exit(code)
}

func ensureRabbit(t *testing.T) string {
	t.Helper()

	itOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		req := testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-management",
			ExposedPorts: []string{"5672/tcp"},
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER": "test",
				"RABBITMQ_DEFAULT_PASS": "test",
			},
			WaitingFor: wait.ForListeningPort("5672/tcp").WithStartupTimeout(90 * time.Second),
		}

		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			itInitErr = fmt.Errorf("start rabbitmq container: %w", err)
			return
		}

		host, err := c.Host(ctx)
		if err != nil {
			itInitErr = fmt.Errorf("resolve rabbitmq host: %w", err)
			_ = c.Terminate(ctx)
			return
		}

		port, err := c.MappedPort(ctx, "5672/tcp")
		if err != nil {
			itInitErr = fmt.Errorf("resolve rabbitmq mapped port: %w", err)
			_ = c.Terminate(ctx)
			return
		}

		itContainer = c
		itAMQPURL = fmt.Sprintf("amqp://test:test@%s:%s/", host, port.Port())
	})

	if itInitErr != nil {
		t.Skipf("integration environment unavailable: %v", itInitErr)
	}

	return itAMQPURL
}

func uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	clean := strings.NewReplacer("/", "_", " ", "_", "-", "_").Replace(t.Name())
	return fmt.Sprintf("%s_%s_%d", prefix, clean, itNameSeq.Add(1))
}

func newTestClient(t *testing.T, exchange string, routingKey string, publishPoolSize int, prefetch int) *Client {
	t.Helper()

	cfg := RabbitMQConfig{
		URL:                         ensureRabbit(t),
		PublishPoolSize:             publishPoolSize,
		ConsumerPrefetch:            prefetch,
		ConnTimeoutSeconds:          10,
		ReconnectBackoffBaseSeconds: 1,
		ReconnectBackoffCapSeconds:  2,
		ReconnectJitterPercent:      0,
		DefaultDeliveryExchange:     exchange,
		DefaultDeliverRoutingKey:    routingKey,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := NewClient(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func openTestChannel(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(ensureRabbit(t))
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("open channel: %v", err)
	}

	t.Cleanup(func() {
		_ = ch.Close()
		_ = conn.Close()
	})

	return conn, ch
}

func runConsumers(t *testing.T, client *Client, specs ...ConsumerSpec) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- client.RunWithConsumers(ctx, specs...)
	}()

	// Fail fast on immediate startup errors (topology, QoS, consume, etc.).
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run consumers startup failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})

	return cancel, done
}

func eventually(t *testing.T, timeout time.Duration, interval time.Duration, fn func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(interval)
	}

	t.Fatalf("eventually timed out: %s", msg)
}

func queueDepth(ch *amqp.Channel, queue string) (int, error) {
	_ = ch
	if itAMQPURL == "" {
		return 0, fmt.Errorf("rabbitmq url is not initialized")
	}

	conn, err := amqp.Dial(itAMQPURL)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	inspectCh, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer inspectCh.Close()

	q, err := inspectCh.QueueInspect(queue)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

func waitUntilQueueDepth(t *testing.T, ch *amqp.Channel, queue string, depth int, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, 50*time.Millisecond, func() bool {
		n, err := queueDepth(ch, queue)
		if err != nil {
			return false
		}
		return n == depth
	}, fmt.Sprintf("queue=%s depth=%d", queue, depth))
}

func waitUntilQueueExists(t *testing.T, ch *amqp.Channel, queue string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, 50*time.Millisecond, func() bool {
		_, err := queueDepth(ch, queue)
		return err == nil
	}, fmt.Sprintf("queue %s exists", queue))
}

func publishRaw(t *testing.T, ch *amqp.Channel, exchange string, routingKey string, body []byte, headers amqp.Table) {
	t.Helper()
	err := ch.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now().UTC(),
		MessageId:     uuid.NewString(),
		CorrelationId: uuid.NewString(),
		Headers:       headers,
		Body:          body,
	})
	if err != nil {
		t.Fatalf("publish raw: %v", err)
	}
}

func publishEnvelope(t *testing.T, client *Client, exchange string, routingKey string, suffix string) error {
	t.Helper()
	return client.PublishJSON(context.Background(), exchange, routingKey, common.Envelope{
		Meta: common.Meta{
			ID:            fmt.Sprintf("id-%s-%d", suffix, time.Now().UnixNano()),
			CorrelationID: fmt.Sprintf("corr-%s-%d", suffix, time.Now().UnixNano()),
			Type:          "test.event.v1",
			Time:          time.Now().UTC(),
		},
		Data: map[string]any{
			"suffix": suffix,
		},
	})
}
