package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// Channel pool (publisher-only)
// -----------------------------------------------------------------------------

var (
	errPoolClosed = errors.New("channel pool closed")
	errConnClosed = errors.New("amqp connection closed")
)

// ChannelPool keeps a bounded number of channels alive.
// NOTE: This pool is intended for PUBLISHING ONLY.
// Do not use pooled channels for Consume/Qos/Tx or long-lived Notify* listeners.
//
// Invariant: permits roughly tracks number of live channels (idle + borrowed).
// It is adjusted down when a channel is permanently dropped.
//
// Borrow/Return are safe for concurrent use.
type ChannelPool struct {
	conn     *amqp.Connection
	pool     chan *amqp.Channel
	capacity int

	closed  atomic.Bool
	newChMu sync.Mutex
	permits chan struct{}
}

func NewChannelPool(conn *amqp.Connection, capacity int) (*ChannelPool, error) {
	if capacity <= 0 {
		capacity = 16
	}
	return &ChannelPool{
		conn:     conn,
		pool:     make(chan *amqp.Channel, capacity),
		capacity: capacity,
		permits:  make(chan struct{}, capacity),
	}, nil
}

// Borrow returns an exclusive publisher channel. It blocks until a channel is available
// or ctx is done. retryDelayMs is kept for backward compatibility and is only used
// as a backoff delay for channel (re)creation failures.
func (cp *ChannelPool) Borrow(ctx context.Context, retryDelayMs int) (*amqp.Channel, error) {
	if cp.closed.Load() {
		return nil, errPoolClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	delay := time.Duration(retryDelayMs) * time.Millisecond
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}

	sleepCtx := func() error {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case ch, ok := <-cp.pool:
			if !ok {
				return nil, errPoolClosed
			}
			if ch == nil {
				cp.releasePermit()
				if err := sleepCtx(); err != nil {
					return nil, err
				}
				continue
			}

			// If the popped channel is unusable, attempt replacement.
			if cp.conn.IsClosed() || ch.IsClosed() {
				_ = SafeClose(ch)

				nch, err := cp.newChannelLocked()
				if err != nil {
					// We permanently dropped one live channel but failed to replace it.
					// Release its permit so the pool can regrow later.
					cp.releasePermit()
					if err := sleepCtx(); err != nil {
						return nil, err
					}
					continue
				}
				return nch, nil
			}

			return ch, nil

		default:
			if cp.conn.IsClosed() {
				return nil, errConnClosed
			}

			// Try to grow by acquiring a permit.
			select {
			case cp.permits <- struct{}{}:
				nch, err := cp.newChannelLocked()
				if err != nil {
					cp.releasePermit()
					if err := sleepCtx(); err != nil {
						return nil, err
					}
					continue
				}
				return nch, nil

			case <-ctx.Done():
				return nil, ctx.Err()

			default:
				// Pool at capacity and no idle channels available; yield briefly.
				if err := sleepCtx(); err != nil {
					return nil, err
				}
			}
		}
	}
}

func (cp *ChannelPool) Return(ch *amqp.Channel) {
	if ch == nil {
		return
	}
	if cp.closed.Load() || cp.conn.IsClosed() || ch.IsClosed() {
		_ = SafeClose(ch)
		cp.releasePermit()
		return
	}
	select {
	case cp.pool <- ch:
	default:
		// pool over capacity: close and release permit
		_ = SafeClose(ch)
		cp.releasePermit()
	}
}

// Discard closes a channel and releases its permit. Use this when a publish operation fails.
func (cp *ChannelPool) Discard(ch *amqp.Channel) {
	if ch != nil {
		_ = SafeClose(ch)
	}
	cp.releasePermit()
}

func (cp *ChannelPool) Close() {
	if cp.closed.Swap(true) {
		return
	}
	close(cp.pool)
	for ch := range cp.pool {
		_ = SafeClose(ch)
		cp.releasePermit()
	}
}

func (cp *ChannelPool) newChannelLocked() (*amqp.Channel, error) {
	cp.newChMu.Lock()
	defer cp.newChMu.Unlock()
	if cp.conn.IsClosed() {
		return nil, errConnClosed
	}
	return cp.conn.Channel()
}

func (cp *ChannelPool) releasePermit() {
	select {
	case <-cp.permits:
	default:
	}
}
