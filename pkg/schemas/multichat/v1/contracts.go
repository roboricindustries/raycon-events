package multichat

type TenantRef struct {
	CompanyID      int64 `json:"company_id"`
	CounterpartyID int64 `json:"counterparty_id"`
}
type ProviderRef struct {
	Provider   string `json:"provider"` // "wa.greenapi","telegram",...
	InstanceID string `json:"instance_id"`
}
type ConversationKey struct {
	ConversationID int64  `json:"conversation_id"`
	ProviderChatID string `json:"provider_chat_id"`
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
