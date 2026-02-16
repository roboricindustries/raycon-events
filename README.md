# raycon-events

#### chanpool usage

```go
lease, err := pool.Lease(ctx)
if err != nil { return err }
defer lease.Release()

ch := lease.Channel()
if err := ch.PublishWithContext(ctx, ex, key, false, false, msg); err != nil {
    lease.Discard() // poison channel on publish error
    return err
}
return nil

```