package botv1

import (
	"testing"

	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

func TestBotOutboundV1Validate(t *testing.T) {
	deliveryClass := "critical"
	msg := BotOutboundV1{
		Tenant: common.TenantRef{
			CompanyID:      1,
			CounterpartyID: 2,
		},
		Provider: common.ProviderRef{
			Provider:   "wa.greenapi",
			InstanceID: 3,
		},
		ClientID: 4,
		Conversation: common.ConversationKey{
			ProviderChatID: "77010000000@c.us",
		},
		InboundProviderMessageID: "in-1",
		OutboundID:               "out-1",
		Kind:                     "bot.greeting",
		Text:                     "hello",
		DeliveryClass:            &deliveryClass,
	}

	if err := msg.Validate(); err != nil {
		t.Fatalf("valid message must pass validation: %v", err)
	}
}

func TestBotOutboundV1ValidateRejectsMissingRequiredFields(t *testing.T) {
	msg := BotOutboundV1{
		Tenant: common.TenantRef{
			CompanyID:      1,
			CounterpartyID: 2,
		},
		Provider: common.ProviderRef{
			Provider:   "wa.greenapi",
			InstanceID: 3,
		},
		ClientID:     4,
		Conversation: common.ConversationKey{},
	}

	if err := msg.Validate(); err == nil {
		t.Fatal("missing required outbound fields must fail validation")
	}
}

func TestBotOutboundV1ValidateRejectsBlankOptionalFieldsWhenPresent(t *testing.T) {
	dep := "   "
	msg := BotOutboundV1{
		Tenant: common.TenantRef{
			CompanyID:      1,
			CounterpartyID: 2,
		},
		Provider: common.ProviderRef{
			Provider:   "wa.greenapi",
			InstanceID: 3,
		},
		ClientID: 4,
		Conversation: common.ConversationKey{
			ProviderChatID: "77010000000@c.us",
		},
		InboundProviderMessageID: "in-1",
		OutboundID:               "out-1",
		Kind:                     "bot.greeting",
		Text:                     "hello",
		DependsOnOutboundID:      &dep,
	}

	if err := msg.Validate(); err == nil {
		t.Fatal("blank depends_on_outbound_id must fail validation")
	}
}
