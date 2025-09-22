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
// Channel pool (lean)
// -----------------------------------------------------------------------------

var (
	errPoolClosed = errors.New("channel pool closed")
	errConnClosed = errors.New("amqp connection closed")
)

// ChannelPool keeps a bounded number of channels alive.
// Invariant: len(permits) == total channels (idle + borrowed) <= capacity.
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

func (cp *ChannelPool) Borrow(ctx context.Context, retryDelayMs int) (*amqp.Channel, error) {
	if cp.closed.Load() {
		return nil, errPoolClosed
	}
	delay := time.Duration(retryDelayMs) * time.Millisecond
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case ch, ok := <-cp.pool:
			if !ok {
				return nil, errPoolClosed
			}
			if cp.conn.IsClosed() || ch.IsClosed() {
				_ = SafeClose(ch)
				nch, err := cp.newChannelLocked()
				if err != nil {
					time.Sleep(delay)
					continue
				}
				return nch, nil
			}
			return ch, nil

		default:
			if cp.conn.IsClosed() {
				return nil, errConnClosed
			}
			// try to grow by acquiring a permit
			select {
			case cp.permits <- struct{}{}:
				nch, err := cp.newChannelLocked()
				if err != nil {
					<-cp.permits // release permit on failure
					time.Sleep(delay)
					continue
				}
				return nch, nil

			case <-ctx.Done():
				return nil, ctx.Err()

			case <-time.After(delay):
				// retry to see if a channel was returned
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
		// if it was borrowed (permit held), try to release a permit
		select {
		case <-cp.permits:
		default:
		}
		return
	}
	select {
	case cp.pool <- ch:
	default:
		// pool over capacity: close and release permit
		_ = SafeClose(ch)
		select {
		case <-cp.permits:
		default:
		}
	}
}

func (cp *ChannelPool) Close() {
	if cp.closed.Swap(true) {
		return
	}
	close(cp.pool)
	for ch := range cp.pool {
		_ = SafeClose(ch)
		select {
		case <-cp.permits:
		default:
		}
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
