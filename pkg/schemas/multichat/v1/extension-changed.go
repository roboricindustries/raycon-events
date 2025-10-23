package multichat

import (
	"encoding/json"
	"time"
)

type ExtensionToggleUpdatedV1 struct {
	Tenant    TenantRef                  `json:"tenant"`
	Feature   string                     `json:"feature"`           // "multichat"|"autodispatch"|"blacklist"|...
	Enabled   *bool                      `json:"enabled,omitempty"` // nil if not boolean
	Options   map[string]json.RawMessage `json:"options,omitempty"` // feature-specific knobs (optional)
	Version   int64                      `json:"version"`           // monotonic (outbox id or UnixNano)
	UpdatedAt time.Time                  `json:"updated_at"`
}
