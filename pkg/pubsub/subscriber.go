package pubsub

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

const (
	defaultBufferCap = 10
	defaultWorkerCnt = 10
)
const (
	defaultDLMessageTTL = 10
	defaultMaxRetry     = 10
)

// matches rabbit msg topics with handler patterns
func matchTopic(pattern, topic string) bool {
	pParts := strings.Split(pattern, ".")
	tParts := strings.Split(topic, ".")

	for i, pi := range pParts {
		if pi == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		if pi != "*" && pi != tParts[i] {
			return false
		}
	}
	return len(pParts) == len(tParts)
}

// retrieve x-death number from a msg for a queue
func GetXDeathCount(msg amqp091.Delivery, queue string) int {
	rawDeaths, ok := msg.Headers["x-death"]
	if !ok {
		return 0
	}
	deaths, ok := rawDeaths.([]any)
	if !ok {
		return 0
	}
	for _, d := range deaths {
		if dmap, ok := d.(amqp091.Table); ok {
			if q, _ := dmap["queue"].(string); q == queue {
				if count, ok := dmap["count"].(int64); ok {
					return int(count)
				}
			}
		}
	}
	return 0
}

// nil ref guard and apply defaults to config
func normalizeConfig(cfg *SubscriberConfig) *SubscriberConfig {
	if cfg == nil {
		cfg = &SubscriberConfig{}
	}
	if cfg.BufferCap == 0 {
		cfg.BufferCap = defaultBufferCap
	}
	if cfg.WorkerCnt == 0 {
		cfg.WorkerCnt = defaultWorkerCnt
	}
	if cfg.Retry.TTL == 0 {
		cfg.Retry.TTL = defaultDLMessageTTL * time.Minute
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = defaultMaxRetry
	}
	return cfg
}

type Handler func(context.Context, amqp091.Delivery) error

type Subscriber interface {
	RegisterHandler(routingKey string, handler Handler) error
	MustRegisterHandler(routingKey string, handler Handler)
	Start() error
	Close() error
	IsConnected() bool
}

// params to use in NewSubscriber
type SubscriberOptions struct {
	URL           string
	Exchange      string
	Module        string
	QueueName     string
	Logger        *slog.Logger
	Config        *SubscriberConfig
	RetryAttempts int
}

type SubscriberConfig struct {
	BufferCap int
	WorkerCnt int
	Retry     RetryPolicy
}

type RetryPolicy struct {
	Enabled     bool
	DLXExchange string
	TTL         time.Duration
	MaxAttempts int
}

// main subscriber struct
type rmqSubscriber struct {
	// config
	config *SubscriberConfig
	log    *slog.Logger

	// rabbit state
	conn *amqp091.Connection
	ch   *amqp091.Channel

	// identifiers
	module        string
	exchange      string
	queue         string
	finalQueue    string
	deadQueue     string
	retryExchange string
	finalExchange string
	deadExchange  string

	//internal state
	handlers map[string]Handler
	msgChan  chan amqp091.Delivery
	done     chan struct{}
	wg       sync.WaitGroup
	once     sync.Once
}

// connect to rabbit mq with retries
func DialWithRetry(url string, attempts int, delay time.Duration, log *slog.Logger) (*amqp091.Connection, error) {
	for i := 1; i <= attempts; i++ {
		conn, err := amqp091.Dial(url)
		if err == nil {
			if i > 1 {
				log.Info("rabbit connected", slog.Int("attempt", i))
			}
			return conn, nil
		}

		log.Warn("rabbit dial failed",
			slog.Int("attempt", i),
			slog.Duration("sleep", delay),
			slog.Any("error", err),
		)

		time.Sleep(delay * time.Duration(math.Pow(2, float64(i))))
	}
	return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts", attempts)
}

// subscriber creation function
func NewSubscriber(options SubscriberOptions) (Subscriber, error) {
	cfg := normalizeConfig(options.Config)
	conn, err := DialWithRetry(options.URL, options.RetryAttempts, time.Second, options.Logger)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.ExchangeDeclare(options.Exchange, "topic", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	var logger *slog.Logger
	if options.Logger != nil {
		logger = options.Logger
	}

	//identifiers
	queueName := options.Module + "." + options.QueueName
	deadQueue := queueName + ".dead"
	finalQueue := queueName + ".final"
	moduleExchange := options.Module + "." + options.Exchange
	deadExchange := moduleExchange + ".dead"
	finalExchange := moduleExchange + ".final"
	retryExchange := moduleExchange + ".retry"

	logger = logger.With(slog.String("module", options.Module), slog.String("queue", queueName), slog.String("exchange", options.Exchange))
	return &rmqSubscriber{
		conn:          conn,
		ch:            ch,
		module:        options.Module,
		exchange:      options.Exchange,
		deadQueue:     deadQueue,
		deadExchange:  deadExchange,
		finalQueue:    finalQueue,
		finalExchange: finalExchange,
		retryExchange: retryExchange,
		config:        cfg,
		log:           logger,
		queue:         queueName,
		handlers:      make(map[string]Handler),
		msgChan:       make(chan amqp091.Delivery, cfg.BufferCap),
		done:          make(chan struct{}),
	}, nil
}

// registers handler for a routing key
func (s *rmqSubscriber) RegisterHandler(routingKey string, handler Handler) error {
	for existingHandler := range s.handlers {
		if matchTopic(routingKey, existingHandler) || matchTopic(existingHandler, routingKey) {
			return fmt.Errorf("conflict: pattern %q overlaps with %q", routingKey, existingHandler)
		}
	}
	s.handlers[routingKey] = handler
	return nil
}

// finds a handler for a msg by its routing key
func (s *rmqSubscriber) getHandler(routingKey string) (Handler, bool) {
	for pattern, handler := range s.handlers {
		if matchTopic(pattern, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

// panics if error on handler register
func (s *rmqSubscriber) MustRegisterHandler(routingKey string, handler Handler) {
	if err := s.RegisterHandler(routingKey, handler); err != nil {
		panic(fmt.Sprintf("failed to register handler for %q: %v", routingKey, err))
	}
}

// main entrypoint to start listening
func (s *rmqSubscriber) Start() error {
	var startErr error
	s.once.Do(func() {
		if err := s.startConsume(); err != nil {
			startErr = err
			return
		}

		s.runWorkerPool()
		s.log.Info("subscriber started")
	})
	return startErr
}

// helper function to bind all routing keys for a queue to an exchange
func (s *rmqSubscriber) bindHandlersToExchange(queueName, exchange string) error {
	for key := range s.handlers {
		if err := s.ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// sends msg to the final queue
func (s *rmqSubscriber) discard(msg amqp091.Delivery) {
	const op = "rmqSubscriber.discard"
	log := s.log.With(slog.String("op", op))

	log.Warn(
		"discarding msg",
		slog.String("key", msg.RoutingKey),
		slog.String("message_id", msg.MessageId),
	)
	err := s.publish(context.Background(), s.finalExchange, msg)
	if err != nil {
		log.Warn("err during publishing to final queue", slog.Any("error", err))
	}
	err = msg.Ack(false)
	if err != nil {
		log.Warn("err during Ack", slog.Any("error", err))
	}
}

// checks if msg should be discarded
func (s *rmqSubscriber) shouldDiscard(msg amqp091.Delivery) bool {
	// if retry enabled, max attemts set and x-death header counter
	// exceeds max attempts
	return s.config.Retry.Enabled &&
		s.config.Retry.MaxAttempts != 0 &&
		GetXDeathCount(msg, s.queue) > s.config.Retry.MaxAttempts
}

// helper function to publish msg to an exchange
func (s *rmqSubscriber) publish(ctx context.Context, exchange string, msg amqp091.Delivery) error {
	return s.ch.PublishWithContext(
		ctx,
		exchange,
		"",
		false, false,
		amqp091.Publishing{
			ContentType:   "application/json",
			Body:          msg.Body,
			MessageId:     msg.MessageId,
			Timestamp:     time.Now(),
			Headers:       msg.Headers,
			CorrelationId: msg.CorrelationId,
			DeliveryMode:  amqp091.Persistent,
			Type:          msg.Type,
			AppId:         msg.AppId,
		},
	)
}

// declares and binds main queue, handles retry logic if on
func (s *rmqSubscriber) setupQueue() error {
	var args amqp091.Table
	if s.config.Retry.Enabled {
		args = amqp091.Table{
			"x-dead-letter-exchange": s.deadExchange,
		}
	}
	q, err := s.ch.QueueDeclare(s.queue, true, false, false, false, args)
	if err != nil {
		return err
	}
	if err := s.bindHandlersToExchange(q.Name, s.exchange); err != nil {
		return err
	}
	if s.config.Retry.Enabled {
		if err := SetupRetryInfra(&RetryConfig{
			Channel:       s.ch,
			DeadExchange:  s.deadExchange,
			FinalExchange: s.finalExchange,
			RetryExchange: s.retryExchange,
			DeadQueue:     s.deadQueue,
			FinalQueue:    s.finalQueue,
			TTL:           s.config.Retry.TTL,
		}); err != nil {
			return err
		}
		if err := s.bindHandlersToExchange(q.Name, s.retryExchange); err != nil {
			return err
		}
	}
	return nil
}

// starts consuming and routing to msg channel
func (s *rmqSubscriber) startConsume() error {
	if err := s.ch.Qos(10, 0, false); err != nil {
		return err
	}

	if err := s.setupQueue(); err != nil {
		return err
	}

	msgs, err := s.ch.Consume(s.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-s.done:
				close(s.msgChan)
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				s.msgChan <- msg
			}
		}
	}()
	return nil
}

// starts worker pool
func (s *rmqSubscriber) runWorkerPool() {
	workerCnt := s.config.WorkerCnt
	if workerCnt == 0 {
		workerCnt = 10
	}
	for i := 0; i < workerCnt; i++ {
		s.wg.Add(1)
		go s.workerLoop()
	}
}

// main msg handler
func (s *rmqSubscriber) workerLoop() {
	const op = "rmqSubscriber.workerLoop"
	log := s.log.With(slog.String("op", op))

	defer s.wg.Done()
	for msg := range s.msgChan {
		if s.config.Retry.Enabled {
			if s.shouldDiscard(msg) {
				s.discard(msg)
				continue
			}
		}
		handler, ok := s.getHandler(msg.RoutingKey)
		if !ok {
			log.Warn("no handler", slog.String("key", msg.RoutingKey))
			s.discard(msg)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := handler(ctx, msg)
		cancel()
		if err != nil {
			log.Error("handler error",
				slog.String("key", msg.RoutingKey),
				slog.String("message_id", msg.MessageId),
				slog.Any("error", err),
			)
			_ = msg.Nack(false, false)
		} else {
			_ = msg.Ack(false)
		}
	}
}

// gracefull shutdown
func (s *rmqSubscriber) Close() error {
	close(s.done)
	s.wg.Wait()
	_ = s.ch.Close()
	return s.conn.Close()
}

// health checks
func (s *rmqSubscriber) IsConnected() bool {
	if s.conn == nil || s.ch == nil {
		return false
	}
	return !s.conn.IsClosed() && !s.ch.IsClosed()
}
