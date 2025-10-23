package multichat

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
