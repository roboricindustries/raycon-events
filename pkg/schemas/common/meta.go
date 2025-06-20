package common

import "time"

type Meta struct {
	// Trace / request correlation ID
	CorrelationID *string `json:"correlation_id,omitempty"`
	// Unique event ID
	ID string `json:"id"`
	// Emitting service and version
	Producer *string `json:"producer,omitempty"`
	// Timestamp when the event was emitted
	Time time.Time `json:"time"`
	// Event name and version, e.g. deals.dispatched.v1
	Type string `json:"type"`
}
