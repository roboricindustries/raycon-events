# raycon-events

### chanpool usage (publisher-only AMQP channel pool)

This package provides a bounded, publisher-only pool of RabbitMQ AMQP channels.

#### Contract (important)

- **Publisher-only**: borrow channel -> `Publish`/`PublishWithContext` -> return/discard.
- **Exclusive-use**: one goroutine owns a borrowed channel at a time.
- Do **NOT** call `Consume`, `Qos`, transactions, or attach long-lived `Notify*` listeners on pooled channels.
- If `Publish` (or any channel operation) returns an error, **Discard** the channel.

Reconnect is out of scope: when your `*amqp.Connection` is replaced, create and swap a new `*chanpool.Pool`.

#### Typical usage

```go
lease, err := pool.Lease(ctx)
if err != nil { return err }
defer lease.Release()

if err := lease.Channel().PublishWithContext(ctx, ex, key, false, false, msg); err != nil {
    lease.Discard()
    return err
}
```

Or via `Do`:

```go
err := pool.Do(ctx, func(ch *amqp.Channel) error {
    return ch.PublishWithContext(ctx, ex, key, false, false, msg)
})
```
