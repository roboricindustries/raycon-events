package common

type EventMeta struct {
	EventType  string // e.g. "deals.dispatched.v1"
	Exchange   string // e.g. "deals"
	RoutingKey string // e.g. "deals.dispatched.v1"
}
