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

// OutboundAttachmentDescriptor describes an attachment for outbound delivery.
// Contains storage_key for worker to fetch bytes from S3.
type OutboundAttachmentDescriptor struct {
	ID         uint64  `json:"id"`                   // attachment DB ID
	Index      int     `json:"index"`                // attachment_index
	Kind       string  `json:"kind"`                 // "image"|"video"|"audio"|"document"
	StorageKey string  `json:"storage_key"`          // required for outbound send
	Mime       *string `json:"mime,omitempty"`       // MIME type
	Filename   *string `json:"filename,omitempty"`   // original filename
	SizeBytes  *int64  `json:"size_bytes,omitempty"` // file size
	Sha256     *string `json:"sha256,omitempty"`     // SHA256 hash
}
