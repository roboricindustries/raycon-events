package multichat

import "time"

// ManagerActivityV1 - We emit on every outgoing message so CRM can create new deal and set bot to ignore the client
type OutboundActivityV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	OutboundID string `json:"outbound_id"`

	Actor     ActorRef  `json:"actor"`
	MessageID *uint64   `json:"message_id,omitempty"`
	MessageAt time.Time `json:"message_at"`
}
