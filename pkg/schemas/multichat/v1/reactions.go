package multichat

import "time"

const (
	EventMessageReactionObservedV1 = "multichat.message.reaction.observed.v1"
)

// ChatMessageReactionObservedV1 represents an observed reaction to a message (incoming only).
type ChatMessageReactionObservedV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	Direction string `json:"direction"` // inbound/outbound (reaction origin)
	Source    string `json:"source"`    // "client"

	Target  MessageKey `json:"target"`
	Emoji   string     `json:"emoji,omitempty"` // empty = remove
	EventID string     `json:"event_id"`        // provider event id (idMessage)

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`
}
