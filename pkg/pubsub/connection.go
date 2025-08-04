package pubsub

import (
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

// connect to rabbit mq with retries
func DialWithRetry(cfg ConnectionOptions) (*amqp091.Connection, error) {
	for i := 1; i <= cfg.RetryAttempts; i++ {
		conn, err := amqp091.Dial(cfg.URL)
		if err == nil {
			if i > 1 {
				cfg.Logger.Info("rabbit connected", slog.Int("attempt", i))
			}
			return conn, nil
		}

		cfg.Logger.Warn("rabbit dial failed",
			slog.Int("attempt", i),
			slog.Duration("sleep", cfg.Delay),
			slog.Any("error", err),
		)

		time.Sleep(cfg.Delay * time.Duration(math.Pow(2, float64(i))))
	}
	return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts", cfg.RetryAttempts)
}
