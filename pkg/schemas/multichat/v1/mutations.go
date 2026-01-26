package multichat

import (
	"encoding/json"
	"time"
)

// ----------------------------------------------------------------------
// Constants (event names)
// ----------------------------------------------------------------------

const (
	EventMutationRequestedV1            = "multichat.mutation.requested.v1"
	EventMutationReceiptV1              = "multichat.mutation.receipt.v1"
	EventMessageEditedObservedV1        = "multichat.message.edited.observed.v1"
	EventMessageRemovedObservedV1       = "multichat.message.removed.observed.v1"
	EventProviderActionStatusObservedV1 = "multichat.provider_action.status.observed.v1" // optional
)

// ----------------------------------------------------------------------
// Enums (stringly typed for compatibility)
// ----------------------------------------------------------------------

type MutationKind string

const (
	MutationEdit   MutationKind = "edit"
	MutationRemove MutationKind = "remove"
)

type MutationReceiptStatus string

const (
	MutationAccepted MutationReceiptStatus = "accepted"
	MutationRejected MutationReceiptStatus = "rejected"
)

// Source aligns with existing observed message sources.
type MutationSource string

const (
	SourceClient MutationSource = "client"
	SourcePhone  MutationSource = "phone"
	SourceEcho   MutationSource = "echo"
	SourceAPI    MutationSource = "api"
)

// ----------------------------------------------------------------------
// 1) HUB -> MTH: mutation request (intent)
// ----------------------------------------------------------------------

// ChatMutationRequestedV1 represents a mutation request from HUB to MTH.
// Validation rules:
// - MutationID is required
// - Kind is required
// - If Kind=edit → Text != nil
type ChatMutationRequestedV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	// MutationID is HUB-generated UUID/ULID. Idempotency + cross-service correlation.
	MutationID string `json:"mutation_id"`

	Kind MutationKind `json:"kind"` // "edit" | "remove"

	// Correlation helpers (at least one should be present in practice)
	MessageID  *uint64     `json:"message_id,omitempty"`  // HUB message_id (optional)
	OutboundID *string     `json:"outbound_id,omitempty"` // if used in your pipeline
	Target     *MessageKey `json:"target,omitempty"`      // provider_message_id (stanzaId) if known

	Actor ActorRef `json:"actor"`

	// For edits only (omit for remove)
	Text *string `json:"text,omitempty"`

	// Optional: helps match echo if needed
	ExpectedFingerprint *string `json:"expected_fingerprint,omitempty"`

	AtHub time.Time `json:"at_hub"`
}

// ----------------------------------------------------------------------
// 2) MTH -> HUB: mutation receipt (fast ack)
// ----------------------------------------------------------------------

// MutationError represents an error in mutation processing.
type MutationError struct {
	Code    string `json:"code"`
	Details string `json:"details,omitempty"`
}

// ChatMutationReceiptV1 represents a mutation receipt from MTH to HUB.
// Validation rules:
// - MutationID is required
// - Status is required
type ChatMutationReceiptV1 struct {
	Tenant   TenantRef   `json:"tenant"`
	Provider ProviderRef `json:"provider"`

	MutationID string                `json:"mutation_id"`
	Status     MutationReceiptStatus `json:"status"` // accepted|rejected

	AtProcessor time.Time `json:"at_processor"`

	// ProviderActionID is the provider-side id of the edit/delete action (GreenAPI idMessage for edit action).
	// For GreenAPI delete REST, it may be empty (response body is empty).
	ProviderActionID *string `json:"provider_action_id,omitempty"`

	Error *MutationError `json:"error,omitempty"`
}

// ----------------------------------------------------------------------
// 3) EAR -> HUB: observed edit/remove (authoritative state application)
// ----------------------------------------------------------------------

// ChatMessageEditedObservedV1 represents an observed message edit from EAR to HUB.
// Validation rules:
// - Target.ProviderMessageID is required
// - Text is required (for edit)
type ChatMessageEditedObservedV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	Direction string        `json:"direction"` // inbound|outbound
	Source MutationSource `json:"source"` // client|phone|echo|api

	// Target is the ORIGINAL message being edited (GreenAPI stanzaId)
	Target MessageKey `json:"target"`

	// ProviderActionID is the provider-side event/action id for this edit (GreenAPI webhook idMessage)
	ProviderActionID *string `json:"provider_action_id,omitempty"`

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`

	Actor ActorRef `json:"actor"`

	Text string         `json:"text"`
	Body BodyDescriptor `json:"body"`

	ProviderMeta json.RawMessage `json:"provider_meta,omitempty"`
}

// ChatMessageRemovedObservedV1 represents an observed message removal from EAR to HUB.
// Validation rules:
// - Target.ProviderMessageID is required
type ChatMessageRemovedObservedV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	Direction string        `json:"direction"` // inbound|outbound
	Source MutationSource `json:"source"` // client|phone|echo|api

	// Target is the ORIGINAL message being deleted (GreenAPI stanzaId)
	Target MessageKey `json:"target"`

	// ProviderActionID is the provider-side event/action id for this delete (GreenAPI webhook idMessage)
	ProviderActionID *string `json:"provider_action_id,omitempty"`

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`

	Actor ActorRef `json:"actor"`

	ProviderMeta json.RawMessage `json:"provider_meta,omitempty"`
}

// ----------------------------------------------------------------------
// 4) EAR -> HUB: provider action status observed (optional, for edit failures)
// ----------------------------------------------------------------------

// ProviderActionStatusObservedV1 represents an observed provider action status from EAR to HUB.
// This event is optional but strongly recommended for handling GreenAPI edit failures.
type ProviderActionStatusObservedV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	// ProviderActionID corresponds to GreenAPI outgoingMessageStatus.idMessage
	ProviderActionID string `json:"provider_action_id"`

	// Provider status string (e.g. sent|delivered|read|failed)
	Status string `json:"status"`

	Description *string `json:"description,omitempty"`

	AtProvider time.Time `json:"at_provider"`
	ReceivedAt time.Time `json:"received_at"`

	ProviderMeta json.RawMessage `json:"provider_meta,omitempty"`
}
