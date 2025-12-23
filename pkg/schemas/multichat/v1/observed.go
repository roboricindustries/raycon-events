package multichat

import (
	"encoding/json"
	"time"
)

// ChatObservedMessageV1 is an extension of ChatInboundV1 with additional context about direction and source.
// This allows unified handling of both inbound and outbound messages.
type ChatObservedMessageV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	Direction string `json:"direction"` // "inbound" | "outbound"
	Source    string `json:"source"`    // "client" | "phone" | "echo" | "api"

	Message MessageKey `json:"message"` // provider message id (required)
	Kind    string     `json:"kind"`    // "text","image","voice","file","system","interactive"

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`

	Body        BodyDescriptor         `json:"body"`
	Attachments []AttachmentDescriptor `json:"attachments,omitempty"`

	ProviderMeta        json.RawMessage `json:"provider_meta,omitempty"`         // optional: raw provider payload
	ProviderMessageType *string         `json:"provider_message_type,omitempty"` // e.g. GreenAPI TypeMessage
}
