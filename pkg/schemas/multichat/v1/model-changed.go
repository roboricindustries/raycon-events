package multichat

import "encoding/json"

type Aggregate struct {
	Type    string `json:"type"`    // e.g. "deal" | "company" | "manager"
	Version int64  `json:"version"` // monotonically increasing
	ID      uint64 `json:"id"`
}

type ModelChangedV1 struct {
	Tenant    TenantRef      `json:"tenant"`
	Aggregate Aggregate      `json:"aggregate"`
	Kind      string         `json:"kind"` // create|update|delete
	Patch     map[string]any `json:"patch"`
}

type AggregateRef struct {
	ID  *uint64   `json:"id,omitempty"`
	Key []KeyPart `json:"key,omitempty"`
}

type KeyPart struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ModelChangedV2 struct {
	Model        string          `json:"model"`
	Op           string          `json:"op"` // operation
	Version      uint64          `json:"version"`
	AggregateRef AggregateRef    `json:"aggregate_ref"`
	TenantRef    *TenantRef      `json:"tenant_ref,omitempty"`
	Doc          json.RawMessage `json:"doc,omitempty"`
}
