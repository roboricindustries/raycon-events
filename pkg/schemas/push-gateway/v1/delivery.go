package pushgateway

import (
	"encoding/json"
	"time"

	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

type PushDeliveryRequestV1 struct {
	CompanyID      *int64 `json:"company_id,omitempty"`
	CounterpartyID *int64 `json:"counterparty_id,omitempty"`

	EventID       string     `json:"event_id"` // Meta.ID (globally unique)
	PrincipalID   int64      `json:"principal_id"`
	PrincipalKind string     `json:"principal_role"`
	ScheduledAt   *time.Time `json:"scheduled_at,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`

	Message PushMessage `json:"message"`

	// Provider-specific knobs. options["webpush"] -> WebPushOptions, others under their namespaces.
	Options map[string]json.RawMessage `json:"options,omitempty"`

	// Routing semantics (who should receive). Core contract (validated).
	Targeting *common.Targeting `json:"targeting,omitempty"`

	// Free-form metadata for producers/consumers (NOT used for routing/credential selection).
	Meta map[string]json.RawMessage `json:"meta,omitempty"`
}

type PushMessage struct {
	Title    string            `json:"title"`
	Body     string            `json:"body"`
	Deeplink string            `json:"deeplink,omitempty"`
	Tag      string            `json:"tag,omitempty"`
	Data     map[string]string `json:"data,omitempty"` // keep identifiers only
}

type WebPushOptions struct {
	TTLSeconds         *int    `json:"ttl_seconds,omitempty"`
	Urgency            *string `json:"urgency,omitempty"`
	SuppressIfActive   *bool   `json:"suppress_if_active,omitempty"`   // passed through, not enforced
	ActiveGraceSeconds *int    `json:"active_grace_seconds,omitempty"` // passed through, not enforced
	// actions/icons/etc as needed
}
