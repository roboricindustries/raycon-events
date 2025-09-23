package multichat

import "time"

type ChatInboundV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`
	Message      MessageKey      `json:"message"`     // may be ""
	Kind         string          `json:"kind"`        // "text","image","voice","file","system","interactive"
	AtProvider   time.Time       `json:"at_provider"` // provider timestamp, if given
	ReceivedAt   time.Time       `json:"received_at"` // when Receiver emitted
	Body         BodyDescriptor  `json:"body"`        // headers-only snapshot
}
