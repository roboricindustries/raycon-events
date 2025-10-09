package multichat

import "time"

type ChatEgressCorrelationV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`
	OutboundID   string          `json:"outbound_id"`
	Message      MessageKey      `json:"message"`
	AtProcessor  time.Time       `json:"at_processor"`
}
