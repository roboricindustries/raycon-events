package events

type TenantRef struct {
	CompanyID      uint64 `json:"company_id"`
	CounterpartyID uint64 `json:"counterparty_id"`
}
type BotIgnore struct {
	Tenant     TenantRef `json:"tenant"`
	DealID     uint64    `json:"deal_id"`
	ClientID   uint64    `json:"client_id"`
	InstanceID uint64    `json:"instance_id"`
}
