package pubsub

import (
	"context"
	"log/slog"

	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

type FallbackPublisher struct {
	log *slog.Logger
}

func (p *FallbackPublisher) Publish(ctx context.Context, key string, msg common.Envelope) error {
	p.log.Warn("FallbackPublisher: skipped publish", slog.String("key", key))
	return nil
}

func (p *FallbackPublisher) Close() error {
	return nil
}

func NewFallback(logger *slog.Logger) Publisher {
	return &FallbackPublisher{
		log: logger,
	}
}
