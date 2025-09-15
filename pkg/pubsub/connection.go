package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type ConnectionOptions struct {
	URL           string
	RetryAttempts int
	Delay         time.Duration
	Logger        *slog.Logger
}

const MaxDelay = 60 //seconds

// DialWithRetry tries to connect to RabbitMQ with exponential backoff.
// It respects context cancellation for graceful shutdown.
func DialWithRetry(ctx context.Context, cfg ConnectionOptions) (*amqp091.Connection, error) {
	var lastErr error

	for i := 1; i <= cfg.RetryAttempts; i++ {
		conn, err := amqp091.Dial(cfg.URL)
		if err == nil {
			if i > 1 {
				cfg.Logger.Info("rabbit connected", slog.Int("attempt", i))
			}
			return conn, nil
		}
		lastErr = err

		// exponential backoff with cap
		sleep := cfg.Delay * time.Duration(math.Pow(2, float64(i-1)))
		if MaxDelay > 0 && sleep > MaxDelay {
			sleep = MaxDelay
		}

		cfg.Logger.Warn("rabbit dial failed",
			slog.Int("attempt", i),
			slog.Duration("sleep", sleep),
			slog.Any("error", err),
		)

		// Wait or cancel
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.New("dial cancelled: " + ctx.Err().Error())
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w",
		cfg.RetryAttempts, lastErr)
}
