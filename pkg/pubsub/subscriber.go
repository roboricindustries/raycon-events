package pubsub

import (
	"context"
	"sync"
	"time"

	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

type Subscriber interface {
	RegisterHandler(routingKey string, handler func(context.Context, amqp091.Delivery) error)
	Start(queueName string) error
	Close() error
}

type rmqSubscriber struct {
	conn      *amqp091.Connection
	ch        *amqp091.Channel
	exchange  string
	log       *slog.Logger
	handlers  map[string]func(context.Context, amqp091.Delivery) error
	msgChan   chan amqp091.Delivery
	done      chan struct{}
	wg        sync.WaitGroup
	once      sync.Once
	bufferCap int
	workerCnt int
}

func NewSubscriber(url, exchange string, logger *slog.Logger, bufferCap, workerCnt int) (Subscriber, error) {
	conn, err := amqp091.Dial(url)
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
	return &rmqSubscriber{
		conn:      conn,
		ch:        ch,
		exchange:  exchange,
		log:       logger,
		handlers:  make(map[string]func(context.Context, amqp091.Delivery) error),
		msgChan:   make(chan amqp091.Delivery, bufferCap),
		done:      make(chan struct{}),
		bufferCap: bufferCap,
		workerCnt: workerCnt,
	}, nil
}

func (s *rmqSubscriber) RegisterHandler(routingKey string, handler func(context.Context, amqp091.Delivery) error) {
	s.handlers[routingKey] = handler
}

func (s *rmqSubscriber) Start(queueName string) error {
	var startErr error
	s.once.Do(func() {
		if err := s.setupQueue(queueName); err != nil {
			startErr = err
			return
		}

		s.runWorkerPool()
		s.log.Info("subscriber started", slog.String("queue", queueName))
	})
	return startErr
}

func (s *rmqSubscriber) setupQueue(queueName string) error {
	if err := s.ch.Qos(10, 0, false); err != nil {
		return err
	}
	q, err := s.ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}
	for key := range s.handlers {
		if err := s.ch.QueueBind(q.Name, key, s.exchange, false, nil); err != nil {
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
	for i := 0; i < s.workerCnt; i++ {
		s.wg.Add(1)
		go s.workerLoop()
	}
}

func (s *rmqSubscriber) workerLoop() {
	defer s.wg.Done()
	for msg := range s.msgChan {
		handler, ok := s.handlers[msg.RoutingKey]
		if !ok {
			s.log.Warn("no handler", slog.String("key", msg.RoutingKey))
			_ = msg.Nack(false, false)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := handler(ctx, msg)
		cancel()
		if err != nil {
			s.log.Error("handler error", slog.String("key", msg.RoutingKey), slog.Any("err", err))
			_ = msg.Nack(false, true)
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
