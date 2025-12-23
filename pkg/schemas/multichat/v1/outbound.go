package multichat

import (
	"encoding/json"
	"time"
)

type ChatOutboundToProcessorV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	OutboundID string    `json:"outbound_id"` // required (UUID)
	Seq        int64     `json:"seq"`
	Kind       string    `json:"kind"`   // "text" (MVP)
	Text       string    `json:"text"`   // full text to send
	AtHub      time.Time `json:"at_hub"` // enqueue timestamp

	// Attachments for outbound messages
	Attachments []OutboundAttachmentDescriptor `json:"attachments,omitempty"`

	// Raw provider metadata
	ProviderMeta json.RawMessage `json:"provider_meta,omitempty"`
}
