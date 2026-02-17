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

const finalExtra = 3

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
	gen uint64,
	logger *slog.Logger,
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
					err := safeConsume(dctx, spec, d, logger)
					select {
					case results <- consumeResult{d: d, err: err}:
					case <-dctx.Done():
						// best-effort, don't block (prevents goroutine leak on closeCh path)
						select {
						case results <- consumeResult{d: d, err: err}:
						default:
						}
						return
					}
				}
			}
		}(laneChans[i])
	}

	finalize := func(d amqp.Delivery, err error) {
		switch {
		case errors.Is(err, ErrPoison):
			if spec.PoisonToFinal {
				finalEx := FirstNonEmpty(TryFinalEx(spec), spec.Queue+".final")
				if err := PublishFinal(ch, finalEx, d); err != nil {
					if logger != nil {
						logger.Error("publish final failed for poison; requeueing",
							slog.Any("error", err), slog.String("queue", spec.Queue))
					}
					if spec.Retry != nil && spec.Retry.Enabled {
						_ = d.Nack(false, false)
					} else {
						if ackAfterSingleRedelivery(logger, d, spec.Queue, "poison-final-publish-failed") {
							return
						}
						_ = d.Nack(false, true)
					}
					return
				}
			}
			_ = d.Ack(false)
			return

		case err != nil:
			if spec.Retry != nil && spec.Retry.Enabled {
				_ = d.Nack(false, false) // to DLX
			} else {
				if ackAfterSingleRedelivery(logger, d, spec.Queue, "retry-disabled-handler-error") {
					return
				}
				_ = d.Nack(false, true) // requeue
			}
			return

		default:
			_ = d.Ack(false)
		}
	}

	if logger != nil {
		logger.Info(
			"consumer dispatch enabled",
			slog.String("name", spec.Name),
			slog.String("queue", spec.Queue),
			slog.Int("lanes", lanes),
			slog.Int("prefetch", pf),
			slog.Int("max_in_flight", maxInFlight),
			slog.Int("lane_queue", laneQ),
			slog.Uint64("gen", gen),
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
		// Drain completed results first to reduce ACK latency under load.
		for {
			select {
			case res := <-results:
				if inUse > 0 {
					inUse--
				}
				finalize(res.d, res.err)
			default:
				goto drained
			}
		}
	drained:

		// Only read new deliveries when we have capacity.
		var msgCh <-chan amqp.Delivery
		if inUse < maxInFlight {
			msgCh = msgs
		}

		select {
		case <-ctx.Done():
			// Graceful shutdown: stop lanes and finalize anything already processed.
			cancel() // stop workers from reading more
			for i := 0; i < lanes; i++ {
				close(laneChans[i])
			}

			// Close results only after workers exit (no more senders).
			go func() {
				wg.Wait()
				close(results)
			}()

			// Drain completed results and finalize (Ack/Nack) them.
			for res := range results {
				inUse--
				finalize(res.d, res.err)
			}

			_ = ch.Close()
			return

		case <-closeCh:
			// Channel closing: schedule restart.
			stopWorkers(2 * time.Second)
			drainMsgsRequeue(msgs)
			c.notifyConsumerClosed(ctx, spec.Name, gen)
			_ = ch.Close()
			return

		case res := <-results:
			if inUse > 0 {
				inUse--
			}
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
				dc := DeathCount(d, spec.Queue)
				if dc >= spec.Retry.MaxAttempts {
					finalEx := FirstNonEmpty(spec.Retry.FinalExchange, spec.Queue+".final")

					if err := PublishFinal(ch, finalEx, d); err != nil {

						if dc >= spec.Retry.MaxAttempts+finalExtra {
							if logger != nil {
								logger.Error("final publish keeps failing; dropping message to avoid infinite loop",
									slog.Any("error", err),
									slog.Int("death_count", dc),
									slog.String("queue", spec.Queue),
								)
							}
							_ = d.Ack(false)

							inUse--
							continue
						} else {
							if logger != nil {
								logger.Error("publish final failed; sending back to retry (DLX)",
									slog.Any("error", err),
									slog.String("queue", spec.Queue))
							}
							_ = d.Nack(false, false)
							inUse--
							continue
						}
					}

					_ = d.Ack(false)
					inUse--
					continue
				}
			}

			key, kerr := ds.KeyFunc(d)
			if kerr != nil {
				if logger != nil {
					logger.Warn("dispatch key extraction failed", slog.String("name", spec.Name), slog.Any("error", kerr))
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
					if inUse > 0 {
						inUse--
					}
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
					c.notifyConsumerClosed(ctx, spec.Name, gen)
					_ = ch.Close()
					return
				}
			}
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
