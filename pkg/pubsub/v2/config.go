package pubsub

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConfig defines client config and topology defaults
type RabbitMQConfig struct {
	URL                         string
	Exchanges                   map[string]string
	Queues                      map[string]string
	RoutingKeys                 map[string]string
	PublishPoolSize             int
	ConsumerPrefetch            int
	ConnTimeoutSeconds          int
	PoolRetryDelayMs            int
	ReconnectBackoffBaseSeconds int
	ReconnectBackoffCapSeconds  int
	ReconnectJitterPercent      int
	Dialer                      func(ctx context.Context, url string) (*amqp.Connection, error)

	// Conventional defaults for your topology
	DefaultGatewayExchange   string
	DefaultDeliveryExchange  string
	DefaultInboundQueue      string
	DefaultStatusQueue       string
	DefaultInboundRoutingKey string
	DefaultStatusRoutingKey  string
	DefaultDeliverRoutingKey string
}
