package multichat

type TenantRef struct {
	CompanyID      uint64 `json:"company_id"`
	CounterpartyID uint64 `json:"counterparty_id"`
}
type ProviderRef struct {
	Provider   string `json:"provider"` // "wa.greenapi","telegram",...
	InstanceID uint64 `json:"instance_id"`
}
type ConversationKey struct {
	ConversationID *uint64 `json:"conversation_id"`
	ProviderChatID string  `json:"provider_chat_id"`
}
type MessageKey struct {
	ProviderMessageID string `json:"provider_message_id"`
}
type BodyDescriptor struct {
	HasText     bool     `json:"has_text"`
	TextPreview string   `json:"text_preview,omitempty"` // ≤512B
	MediaKinds  []string `json:"media_kinds,omitempty"`  // ["image","audio"]
	Fingerprint string   `json:"fingerprint,omitempty"`
}

type ActorRef struct {
	// Principal kind / role in your auth model.
	// Examples: "manager", "company", "counterparty", "moderator", "admin"
	Kind string `json:"kind"`

	// ID in that principal table/domain
	ID uint64 `json:"id"`
}
