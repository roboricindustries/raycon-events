package multichat

import "time"

type ChatAssignmentV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	ManagerID  uint64    `json:"manager_id"`
	DealID     uint64    `json:"deal_id"`
	ClientID   uint64    `json:"client_id,omitempty"` // advisory
	AssignedAt time.Time `json:"assigned_at"`
}
