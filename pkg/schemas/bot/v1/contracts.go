package botv1

import (
	"fmt"
	"strings"
	"time"

	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

type BotInboundV1 struct {
	Tenant       common.TenantRef       `json:"tenant"`
	Provider     common.ProviderRef     `json:"provider"`
	ClientID     uint64                 `json:"client_id"`
	Conversation common.ConversationKey `json:"conversation"`

	Message common.MessageKey `json:"message"`
	Text    string            `json:"text"`
	Link    string            `json:"link,omitempty"`

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`

	ProviderMessageType *string `json:"provider_message_type,omitempty"`
}

type BotOutboundV1 struct {
	Tenant       common.TenantRef       `json:"tenant"`
	Provider     common.ProviderRef     `json:"provider"`
	ClientID     uint64                 `json:"client_id"`
	Conversation common.ConversationKey `json:"conversation"`

	InboundProviderMessageID string `json:"inbound_provider_message_id"`
	OutboundID               string `json:"outbound_id"`
	Kind                     string `json:"kind"`
	Text                     string `json:"text"`

	DependsOnOutboundID *string `json:"depends_on_outbound_id,omitempty"`
	DeliveryClass       *string `json:"delivery_class,omitempty"`
	FlowID              string  `json:"flow_id,omitempty"`
	ClosesFlow          bool    `json:"closes_flow,omitempty"`
}

func (m BotInboundV1) MessageText() string {
	return strings.TrimSpace(m.Text)
}

func (m BotInboundV1) MessageID() string {
	return strings.TrimSpace(m.Message.ProviderMessageID)
}

func (m BotInboundV1) Validate() error {
	if m.Tenant.CompanyID == 0 {
		return fmt.Errorf("tenant.company_id is required")
	}
	if m.Provider.InstanceID == 0 {
		return fmt.Errorf("provider.instance_id is required")
	}
	if m.ClientID == 0 {
		return fmt.Errorf("client_id is required")
	}
	if strings.TrimSpace(m.Provider.Provider) == "" {
		return fmt.Errorf("provider.provider is required")
	}
	if strings.TrimSpace(m.Conversation.ProviderChatID) == "" {
		return fmt.Errorf("conversation.provider_chat_id is required")
	}
	if strings.TrimSpace(m.Message.ProviderMessageID) == "" {
		return fmt.Errorf("message.provider_message_id is required")
	}
	if m.AtProvider.IsZero() {
		return fmt.Errorf("at_provider is required")
	}
	return nil
}

func (m BotOutboundV1) Validate() error {
	if m.Tenant.CompanyID == 0 {
		return fmt.Errorf("tenant.company_id is required")
	}
	if m.Tenant.CounterpartyID == 0 {
		return fmt.Errorf("tenant.counterparty_id is required")
	}
	if strings.TrimSpace(m.Provider.Provider) == "" {
		return fmt.Errorf("provider.provider is required")
	}
	if m.Provider.InstanceID == 0 {
		return fmt.Errorf("provider.instance_id is required")
	}
	if m.ClientID == 0 {
		return fmt.Errorf("client_id is required")
	}
	if strings.TrimSpace(m.Conversation.ProviderChatID) == "" {
		return fmt.Errorf("conversation.provider_chat_id is required")
	}
	if strings.TrimSpace(m.InboundProviderMessageID) == "" {
		return fmt.Errorf("inbound_provider_message_id is required")
	}
	if strings.TrimSpace(m.OutboundID) == "" {
		return fmt.Errorf("outbound_id is required")
	}
	if strings.TrimSpace(m.Kind) == "" {
		return fmt.Errorf("kind is required")
	}
	if strings.TrimSpace(m.Text) == "" {
		return fmt.Errorf("text is required")
	}
	if m.DependsOnOutboundID != nil && strings.TrimSpace(*m.DependsOnOutboundID) == "" {
		return fmt.Errorf("depends_on_outbound_id must be omitted or non-empty")
	}
	if m.DeliveryClass != nil && strings.TrimSpace(*m.DeliveryClass) == "" {
		return fmt.Errorf("delivery_class must be omitted or non-empty")
	}
	if strings.TrimSpace(m.FlowID) == "" && m.ClosesFlow {
		return fmt.Errorf("closes_flow requires flow_id")
	}
	return nil
}
