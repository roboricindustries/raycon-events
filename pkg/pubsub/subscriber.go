package pubsub

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

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

func getXDeathCount(msg amqp091.Delivery, queue string) int {
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

type Handler func(context.Context, amqp091.Delivery) error

type Subscriber interface {
	RegisterHandler(routingKey string, handler Handler) error
	MustRegisterHandler(routingKey string, handler Handler)
	Start() error
	Close() error
}

type SubConfig struct {
	BufferCap    int
	WorkerCnt    int
	DelayRetry   bool
	DLXName      string
	DLMessageTTL int
	MaxRetry     int
}

type rmqSubscriber struct {
	conn      *amqp091.Connection
	ch        *amqp091.Channel
	exchange  string
	log       *slog.Logger
	handlers  map[string]Handler
	msgChan   chan amqp091.Delivery
	done      chan struct{}
	wg        sync.WaitGroup
	once      sync.Once
	queueName string
	config    *SubConfig
}

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

func NewSubscriber(
	url, exchange string,
	queueName string,
	logger *slog.Logger,
	config *SubConfig, retryAttempts int,

) (Subscriber, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	conn, err := DialWithRetry(url, retryAttempts, time.Second, logger)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	bufferCap := config.BufferCap
	if bufferCap == 0 {
		bufferCap = 10
	}
	return &rmqSubscriber{
		conn:      conn,
		ch:        ch,
		exchange:  exchange,
		log:       logger,
		queueName: queueName,
		handlers:  make(map[string]Handler),
		msgChan:   make(chan amqp091.Delivery, bufferCap),
		done:      make(chan struct{}),
		config:    config,
	}, nil
}

func (s *rmqSubscriber) RegisterHandler(routingKey string, handler Handler) error {
	for existingHandler := range s.handlers {
		if matchTopic(routingKey, existingHandler) || matchTopic(existingHandler, routingKey) {
			return fmt.Errorf("conflict: pattern %q overlaps with %q", routingKey, existingHandler)
		}
	}
	s.handlers[routingKey] = handler
	return nil
}

func (s *rmqSubscriber) getHandler(routingKey string) (Handler, bool) {
	for pattern, handler := range s.handlers {
		if matchTopic(pattern, routingKey) {
			return handler, true
		}
	}
	return nil, false
}

func (s *rmqSubscriber) MustRegisterHandler(routingKey string, handler Handler) {
	if err := s.RegisterHandler(routingKey, handler); err != nil {
		panic(fmt.Sprintf("failed to register handler for %q: %v", routingKey, err))
	}
}

func (s *rmqSubscriber) Start() error {
	var startErr error
	s.once.Do(func() {
		if err := s.setupQueue(s.queueName); err != nil {
			startErr = err
			return
		}

		s.runWorkerPool()
		s.log.Info("subscriber started", slog.String("queue", s.queueName))
	})
	return startErr
}

func validateConfig(config *SubConfig) error {
	if config == nil {
		return nil
	}
	if config.DelayRetry {
		if config.DLXName == "" {
			return errors.New("dead letter exchange name should exist")
		}
	}
	return nil
}

func (s *rmqSubscriber) setupRetryQueue(dqueueName string) error {
	if err := s.ch.ExchangeDeclare(
		s.config.DLXName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	ttl := s.config.DLMessageTTL
	if ttl == 0 {
		ttl = 60000 // 10 min
	}
	args := amqp091.Table{
		"x-message-ttl":          ttl,
		"x-dead-letter-exchange": s.exchange,
	}

	dq, err := s.ch.QueueDeclare(dqueueName, true, false, false, false, args)
	if err != nil {
		return err
	}
	if err := s.ch.QueueBind(
		dq.Name, dq.Name, s.config.DLXName, false, nil,
	); err != nil {
		return err
	}
	return nil
}

func (s *rmqSubscriber) setupQueue(queueName string) error {
	if err := s.ch.Qos(10, 0, false); err != nil {
		return err
	}
	dqueueName := "dead_" + queueName
	var args amqp091.Table
	if s.config.DelayRetry {
		args = amqp091.Table{
			"x-dead-letter-exchange": s.config.DLXName,
		}
	}
	q, err := s.ch.QueueDeclare(queueName, true, false, false, false, args)
	if err != nil {
		return err
	}
	for key := range s.handlers {
		if err := s.ch.QueueBind(q.Name, key, s.exchange, false, nil); err != nil {
			return err
		}
	}
	if s.config.DelayRetry {
		if err := s.setupRetryQueue(dqueueName); err != nil {
			return err
		}
		if err := s.ch.QueueBind(q.Name, q.Name, s.config.DLXName, false, nil); err != nil {
			return err
		}
	}
	msgs, err := s.ch.Consume(q.Name, "", false, false, false, false, nil)
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

func (s *rmqSubscriber) workerLoop() {
	defer s.wg.Done()
	for msg := range s.msgChan {
		if (getXDeathCount(msg, s.queueName) > s.config.MaxRetry) && (s.config.MaxRetry != 0) {
			s.log.Warn("discarding message due to retry limit",
				slog.String("key", msg.RoutingKey),
				slog.String("message_id", msg.MessageId),
				slog.Int("retry_count", getXDeathCount(msg, s.queueName)),
			)
			_ = msg.Ack(false) // discard msg
		}
		handler, ok := s.getHandler(msg.RoutingKey)
		if !ok {
			s.log.Warn("no handler", slog.String("key", msg.RoutingKey))
			_ = msg.Nack(false, false)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := handler(ctx, msg)
		cancel()
		if err != nil {
			s.log.Error("handler error",
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

func (s *rmqSubscriber) Close() error {
	close(s.done)
	s.wg.Wait()
	_ = s.ch.Close()
	return s.conn.Close()
}
