package pubsub

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumeResult struct {
	d   amqp.Delivery
	err error
}

// dispatchConsumeLoop runs the consumer in keyed-lane mode.
//
// Invariants:
//   - Only this goroutine performs Ack/Nack/PublishFinal.
//   - Lane workers only call spec.Consume and report results back.
func (c *Client) dispatchConsumeLoop(
	ctx context.Context,
	ch *amqp.Channel,
	spec ConsumerSpec,
	pf int,
	msgs <-chan amqp.Delivery,
	closeCh <-chan *amqp.Error,
) {
	ds := spec.Dispatch
	if ds == nil || ds.Lanes <= 1 || ds.KeyFunc == nil {
		// Should not happen (startConsumer checks), but keep safe.
		_ = ch.Close()
		return
	}

	lanes := ds.Lanes
	maxInFlight := pf
	if ds.MaxInFlight > 0 {
		maxInFlight = ds.MaxInFlight
	}
	if maxInFlight <= 0 {
		maxInFlight = 1
	}

	laneQ := ds.LaneQueueSize
	if laneQ <= 0 {
		laneQ = maxInFlight / lanes
		if laneQ < 1 {
			laneQ = 1
		}
	}
	if laneQ > maxInFlight {
		laneQ = maxInFlight
	}

	// Buffer results so lane workers don't deadlock.
	results := make(chan consumeResult, maxInFlight)

	laneChans := make([]chan amqp.Delivery, lanes)
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < lanes; i++ {
		laneChans[i] = make(chan amqp.Delivery, laneQ)
		wg.Add(1)
		go func(in <-chan amqp.Delivery) {
			defer wg.Done()
			for {
				select {
				case <-dctx.Done():
					return
				case d, ok := <-in:
					if !ok {
						return
					}
					err := spec.Consume(dctx, d)
					select {
					case results <- consumeResult{d: d, err: err}:
					case <-dctx.Done():
						return
					}
				}
			}
		}(laneChans[i])
	}

	notifyClosed := func() {
		select {
		case c.consumerClosed <- spec.Name:
		default:
		}
	}

	finalize := func(d amqp.Delivery, err error) {
		switch {
		case errors.Is(err, ErrPoison):
			if spec.PoisonToFinal {
				finalEx := FirstNonEmpty(TryFinalEx(spec), spec.Queue+".final")
				_ = PublishFinal(ch, finalEx, d)
			}
			_ = d.Ack(false)
			return

		case err != nil:
			if spec.Retry != nil && spec.Retry.Enabled {
				_ = d.Nack(false, false) // to DLX
			} else {
				_ = d.Nack(false, true) // requeue
			}
			return

		default:
			_ = d.Ack(false)
		}
	}

	if c.logger != nil {
		c.logger.Info(
			"consumer dispatch enabled",
			slog.String("name", spec.Name),
			slog.String("queue", spec.Queue),
			slog.Int("lanes", lanes),
			slog.Int("prefetch", pf),
			slog.Int("max_in_flight", maxInFlight),
			slog.Int("lane_queue", laneQ),
		)
	}

	stopWorkers := func(timeout time.Duration) {
		cancel()
		for i := 0; i < lanes; i++ {
			close(laneChans[i])
		}
		if timeout <= 0 {
			return
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			// best-effort
		}
	}

	inUse := 0

	for {
		// Only read new deliveries when we have capacity.
		var msgCh <-chan amqp.Delivery
		if inUse < maxInFlight {
			msgCh = msgs
		}

		select {
		case <-ctx.Done():
			// Shutdown: no restart.
			stopWorkers(0)
			_ = ch.Close()
			return

		case <-closeCh:
			// Channel closing: schedule restart.
			stopWorkers(2 * time.Second)
			drainMsgsRequeue(msgs)
			notifyClosed()
			_ = ch.Close()
			return

		case res := <-results:
			inUse--
			finalize(res.d, res.err)

		case d, ok := <-msgCh:
			if !ok {
				stopWorkers(0)
				_ = ch.Close()
				return
			}

			// Reserve slot.
			inUse++

			// Max-attempts check before dispatch.
			if spec.Retry != nil && spec.Retry.Enabled && spec.Retry.MaxAttempts > 0 {
				if DeathCount(d, spec.Queue) >= spec.Retry.MaxAttempts {
					_ = PublishFinal(ch, FirstNonEmpty(spec.Retry.FinalExchange, spec.Queue+".final"), d)
					_ = d.Ack(false)
					inUse--
					continue
				}
			}

			key, kerr := ds.KeyFunc(d)
			if kerr != nil {
				if c.logger != nil {
					c.logger.Warn("dispatch key extraction failed", slog.String("name", spec.Name), slog.Any("error", kerr))
				}
				finalize(d, ErrPoison)
				inUse--
				continue
			}

			idx := int(hashKey(key) % uint32(lanes))

			// Enqueue without blocking forever; keep draining results while waiting for space.
			enqueued := false
			for !enqueued {
				select {

				case res := <-results:
					inUse--
					finalize(res.d, res.err)
				case laneChans[idx] <- d:
					enqueued = true

				case <-ctx.Done():
					inUse--
					stopWorkers(0)
					_ = ch.Close()
					return

				case <-closeCh:
					inUse--
					stopWorkers(0)
					drainMsgsRequeue(msgs)
					notifyClosed()
					_ = ch.Close()
					return
				}
			}
			// continue
		}
	}
}

func drainMsgsRequeue(msgs <-chan amqp.Delivery) {
	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return
			}
			_ = d.Nack(false, true)
		default:
			return
		}
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
