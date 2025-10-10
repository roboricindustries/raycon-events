package common

import "encoding/json"

type Core struct {
	NotificationCode string `json:"notification_code"`
	CompanyID        *uint  `json:"company_id,omitempty"`
	CounterpartyID   *uint  `json:"counterparty_id,omitempty"`

	RecipientRole RecipientRole `json:"recipient_role"`
	RecipientID   uint          `json:"recipient_id"`
	Body          string        `json:"body"`

	Options map[string]json.RawMessage `json:"options,omitempty"`
	Meta    map[string]any             `json:"meta"`
}

type RecipientRole string

const (
	Client  RecipientRole = "client"
	Manager RecipientRole = "manager"
	User    RecipientRole = "user"
)
