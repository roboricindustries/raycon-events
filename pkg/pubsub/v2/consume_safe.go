package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	amqp "github.com/rabbitmq/amqp091-go"
)

// safeConsume executes the consumer handler and converts panics into ordinary errors.
// This prevents a single buggy handler from killing the consumer goroutine (legacy mode)
// or wedging a lane (dispatch mode).
func safeConsume(ctx context.Context, spec ConsumerSpec, d amqp.Delivery, logger *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in consumer %q: %v", spec.Name, r)
			if logger != nil {
				logger.Error(
					"consumer panic",
					slog.String("name", spec.Name),
					slog.String("queue", spec.Queue),
					slog.Any("recover", r),
					slog.String("stack", string(debug.Stack())),
				)
			}
		}
	}()
	return spec.Consume(ctx, d)
}
