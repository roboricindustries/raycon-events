// Package chanpool provides a bounded, publisher-only pool of AMQP channels.
//
// Contract (publisher-only):
//   - Borrowers must NOT call Consume/Qos/Tx/Flow or attach long-lived Notify* listeners.
//   - Borrowers must treat channels as exclusive-use (one goroutine at a time).
//   - If Publish returns an error (or you suspect channel corruption), call Discard() (or Lease.Discard()).
//   - Reconnect is intentionally out of scope: swap the whole pool when the connection is replaced.
package chanpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var ErrPoolClosed = errors.New("amqp channel pool is closed")

// Option configures the Pool.
type Option func(*Pool)

// WithWarmup pre-creates up to size channels during New() (fail-fast).
func WithWarmup() Option { return func(p *Pool) { p.warmup = true } }

// WithConfigure runs fn on every newly created channel (warmup + replacements).
// Typical publisher-only use: enable confirm mode, declare exchanges (if you do it here), etc.
func WithConfigure(fn func(*amqp091.Channel) error) Option {
	return func(p *Pool) { p.configure = fn }
}

// WithValidate runs fn on a channel popped from idle before reusing it.
// Return false to force discard+recreate. Optional; publisher-only usually doesn't need it.
func WithValidate(fn func(*amqp091.Channel) bool) Option {
	return func(p *Pool) { p.validate = fn }
}

// Pool is a bounded pool of AMQP channels.
// It assumes the *amqp091.Connection lifecycle/reconnect logic is handled elsewhere.
type Pool struct {
	conn   *amqp091.Connection
	size   int
	idle   chan *amqp091.Channel
	tokens chan struct{} // semaphore: limits checked-out channels to size

	closeCh   chan struct{}
	closeOnce sync.Once

	warmup    bool
	configure func(*amqp091.Channel) error
	validate  func(*amqp091.Channel) bool
}

// New creates a bounded pool of size channels.
func New(conn *amqp091.Connection, size int, opts ...Option) (*Pool, error) {
	if conn == nil {
		return nil, errors.New("nil connection")
	}
	if size <= 0 {
		return nil, errors.New("size must be > 0")
	}
	if conn.IsClosed() {
		return nil, errors.New("connection is closed")
	}

	p := &Pool{
		conn:    conn,
		size:    size,
		idle:    make(chan *amqp091.Channel, size),
		tokens:  make(chan struct{}, size),
		closeCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	// Fill semaphore tokens.
	for i := 0; i < size; i++ {
		p.tokens <- struct{}{}
	}

	// Optional warmup: fail fast at startup.
	if p.warmup {
		created := make([]*amqp091.Channel, 0, size)
		for i := 0; i < size; i++ {
			ch, err := p.newChannel()
			if err != nil {
				// Best-effort cleanup.
				for _, c := range created {
					_ = c.Close()
				}
				p.Close()
				return nil, err
			}
			created = append(created, ch)
			p.idle <- ch
		}
	}

	return p, nil
}

// Acquire returns an exclusive-use channel, bounded by pool size.
// It respects ctx cancellation/deadlines while waiting for pool capacity.
func (p *Pool) Acquire(ctx context.Context) (*amqp091.Channel, error) {
	// Fast path: closed?
	select {
	case <-p.closeCh:
		return nil, ErrPoolClosed
	default:
	}

	// Take a token (capacity for one checked-out channel).
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closeCh:
		return nil, ErrPoolClosed
	case <-p.tokens:
	}

	// Ensure token returned on any early exit.
	returnToken := true
	defer func() {
		if returnToken {
			p.returnToken()
		}
	}()

	// Closed concurrently?
	select {
	case <-p.closeCh:
		return nil, ErrPoolClosed
	default:
	}

	// Get an idle channel if present; otherwise create lazily.
	var ch *amqp091.Channel
	select {
	case ch = <-p.idle:
	default:
	}

	// Validate / replace closed channel (or create new if nil).
	if ch == nil || ch.IsClosed() || (p.validate != nil && !p.validate(ch)) {
		if ch != nil {
			_ = ch.Close()
		}
		var err error
		ch, err = p.newChannel()
		if err != nil {
			return nil, err
		}
	}

	// Final close check: prevent "Acquire succeeds after Close()" surprises.
	select {
	case <-p.closeCh:
		_ = ch.Close()
		return nil, ErrPoolClosed
	default:
	}

	// Success: caller owns token until Release/Discard.
	returnToken = false
	return ch, nil
}

// AcquireTimeout is a convenience wrapper around Acquire with a background context.
func (p *Pool) AcquireTimeout(timeout time.Duration) (*amqp091.Channel, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.Acquire(ctx)
}

// Release returns the channel to the pool.
// Publisher-only: it NEVER creates replacement channels (so it never blocks).
// If the pool is closed or the channel is bad, it closes the channel and drops it.
func (p *Pool) Release(ch *amqp091.Channel) {
	defer p.returnToken()

	if ch == nil {
		return
	}

	// If pool is closed, just close the channel.
	select {
	case <-p.closeCh:
		_ = ch.Close()
		return
	default:
	}

	// If channel is closed/bad, drop it. Acquire will recreate as needed.
	if ch.IsClosed() || (p.validate != nil && !p.validate(ch)) {
		_ = ch.Close()
		return
	}

	// Non-blocking put-back; if pool is somehow full, close to avoid leaks/deadlocks.
	select {
	case p.idle <- ch:
	default:
		_ = ch.Close()
	}
}

// Discard closes the channel instead of returning it to the pool.
// Use this when Publish (or any channel op) fails, or when you want a fresh channel next time.
func (p *Pool) Discard(ch *amqp091.Channel) {
	defer p.returnToken()
	if ch != nil {
		_ = ch.Close()
	}
}

// Close closes the pool and any currently-idle channels.
// In-flight (checked-out) channels will be closed when released/discarded.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeCh)

		// Drain idle channels and close them.
		for {
			select {
			case ch := <-p.idle:
				if ch != nil {
					_ = ch.Close()
				}
			default:
				return
			}
		}
	})
}

// Stats gives lightweight operational visibility.
type Stats struct {
	Size   int
	Idle   int
	InUse  int
	Closed bool
}

// Stats returns pool size/counters. Values are approximate under concurrency.
func (p *Pool) Stats() Stats {
	closed := false
	select {
	case <-p.closeCh:
		closed = true
	default:
	}
	inUse := p.size - len(p.tokens)
	if inUse < 0 {
		inUse = 0
	}
	return Stats{
		Size:   p.size,
		Idle:   len(p.idle),
		InUse:  inUse,
		Closed: closed,
	}
}

// Lease is an optional high-value helper that prevents forgotten Release() and
// makes "discard on error" patterns easy.
type Lease struct {
	p    *Pool
	ch   *amqp091.Channel
	once sync.Once
}

func (p *Pool) Lease(ctx context.Context) (*Lease, error) {
	ch, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &Lease{p: p, ch: ch}, nil
}

func (l *Lease) Channel() *amqp091.Channel { return l.ch }

// Release returns the channel to the pool (idempotent).
func (l *Lease) Release() {
	l.once.Do(func() { l.p.Release(l.ch) })
}

// Discard closes the channel and returns capacity to the pool (idempotent).
func (l *Lease) Discard() {
	l.once.Do(func() { l.p.Discard(l.ch) })
}

// Do is an optional helper: runs fn with a leased channel.
// If fn returns error -> Discard; otherwise -> Release.
func (p *Pool) Do(ctx context.Context, fn func(*amqp091.Channel) error) error {
	l, err := p.Lease(ctx)
	if err != nil {
		return err
	}
	defer func() {
		// default to discard on panic
		if r := recover(); r != nil {
			l.Discard()
			panic(r)
		}
	}()
	if err := fn(l.Channel()); err != nil {
		l.Discard()
		return err
	}
	l.Release()
	return nil
}

func (p *Pool) newChannel() (*amqp091.Channel, error) {
	if p.conn.IsClosed() {
		return nil, errors.New("connection is closed")
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, err
	}
	if p.configure != nil {
		if err := p.configure(ch); err != nil {
			_ = ch.Close()
			return nil, err
		}
	}
	return ch, nil
}

func (p *Pool) returnToken() {
	// Normal path: token channel should have room.
	select {
	case p.tokens <- struct{}{}:
	default:
		// Indicates a bug (e.g., Release/Discard called more times than Acquire).
		// Intentionally non-blocking and non-panicking.
	}
}
