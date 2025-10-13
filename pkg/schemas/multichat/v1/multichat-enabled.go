package multichat

import (
	"encoding/json"
	"time"
)

type MultichatStatusUpdate struct {
	Tenant  TenantRef `json:"tenant"`
	Enabled bool      `json:"enabled"`
	Version int64     `json:"version"`

	Options map[string]json.RawMessage `json:"options,omitempty"`

	UpdatedAt time.Time `json:"updated_at"`
}
