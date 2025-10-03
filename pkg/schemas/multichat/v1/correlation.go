package multichat

import "time"

type ChatEgressCorrelationV1 struct {
	Tenant       TenantRef
	Provider     ProviderRef     // includes instance_id
	Conversation ConversationKey // include provider_chat_id if you have it
	OutboundID   string
	Message      MessageKey // provider_message_id
	AtProcessor  time.Time
}
