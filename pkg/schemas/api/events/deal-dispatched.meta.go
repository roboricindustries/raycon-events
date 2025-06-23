package events

import "github.com/roboricindustries/raycon-events/pkg/schemas/common"

const (
	EventType  = "deals.dispatched.v1"
	Exchange   = "api.events"
	RoutingKey = "deals.dispatched.v1"
)

var DealDispatchedMeta = common.EventMeta{
	EventType:  EventType,
	Exchange:   Exchange,
	RoutingKey: RoutingKey,
}
