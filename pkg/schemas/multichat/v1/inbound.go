package multichat

import (
	"fmt"
	"sort"
	"time"
)

// NEW: AttachmentDescriptor (provider-agnostic)
type AttachmentState string

const (
	AttPending AttachmentState = "pending"
	AttAvail   AttachmentState = "available" // set by media ingestor later
	AttFailed  AttachmentState = "failed"
)

type AttachmentProviderRef struct {
	MessageID       string `json:"message_id"`
	ChatID          string `json:"chat_id"`
	AttachmentIndex int    `json:"attachment_index"`
}

type AttachmentDescriptor struct {
	ID        string          `json:"id"`    // deterministic: <provider_msg_id>#<idx>
	Kind      string          `json:"kind"`  // "image","audio","video","document","sticker","location","contact"
	State     AttachmentState `json:"state"` // always "pending" in the receiver for now
	Filename  string          `json:"filename,omitempty"`
	Mime      string          `json:"mime,omitempty"`
	SizeBytes *int64          `json:"size_bytes,omitempty"`
	// What the future downloader needs to call provider’s media API:
	ProviderRef  AttachmentProviderRef `json:"provider_ref"`
	ProviderMeta map[string]any        `json:"provider_meta,omitempty"` // harmless raw hints (e.g., GA type)
}

type ChatInboundV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`
	Message      MessageKey      `json:"message"`     // may be ""
	Kind         string          `json:"kind"`        // "text","image","voice","file","system","interactive"
	AtProvider   time.Time       `json:"at_provider"` // provider timestamp, if given
	ReceivedAt   time.Time       `json:"received_at"` // when Receiver emitted
	Body         BodyDescriptor  `json:"body"`        // headers-only snapshot

	Attachments []AttachmentDescriptor `json:"attachments,omitempty"`
}

// ---------------- Allowed enums ----------------

var allowedAttachmentKinds = map[string]struct{}{
	"image": {}, "audio": {}, "video": {}, "document": {},
	"sticker": {}, "location": {}, "contact": {},
}

var allowedMessageKinds = map[string]struct{}{
	"text": {}, "image": {}, "voice": {}, "file": {}, "system": {}, "interactive": {},
}

// ---------------- AttachmentDescriptor helpers ----------------

func (a AttachmentDescriptor) Validate() error {
	ve := &ValidationError{}

	// Basic ref checks
	if a.ProviderRef.MessageID == "" {
		ve.add("provider_ref.message_id", "required")
	}
	if a.ProviderRef.ChatID == "" {
		ve.add("provider_ref.chat_id", "required")
	}
	if a.ProviderRef.AttachmentIndex < 0 {
		ve.add("provider_ref.attachment_index", "must be >= 0")
	}
	// Deterministic ID enforcement
	want := DeterministicAttachmentID(a.ProviderRef.MessageID, a.ProviderRef.AttachmentIndex)
	if a.ID != want {
		ve.add("id", fmt.Sprintf("must equal %q", want))
	}
	// Kind & state
	if _, ok := allowedAttachmentKinds[a.Kind]; !ok {
		ve.add("kind", fmt.Sprintf("unsupported: %q", a.Kind))
	}
	switch a.State {
	case AttPending, AttAvail, AttFailed:
	default:
		ve.add("state", fmt.Sprintf("unsupported: %q", a.State))
	}
	// Size sanity (optional/nullable)
	if a.SizeBytes != nil && *a.SizeBytes < 0 {
		ve.add("size_bytes", "must be >= 0")
	}

	if len(ve.Issues) > 0 {
		return ve
	}
	return nil
}

// ---------------- ChatInboundV1 helpers ----------------

func (m *ChatInboundV1) Normalize() {
	// Sort attachments by index for stable ordering
	sort.Slice(m.Attachments, func(i, j int) bool {
		return m.Attachments[i].ProviderRef.AttachmentIndex < m.Attachments[j].ProviderRef.AttachmentIndex
	})
}

func (m ChatInboundV1) Validate() error {
	ve := &ValidationError{}

	// Tenant: at least one id must be set
	if m.Tenant.CompanyID == 0 && m.Tenant.CounterpartyID == 0 {
		ve.add("tenant", "one of company_id or counterparty_id must be > 0")
	}
	if m.Provider.Provider == "" {
		ve.add("provider.provider", "required")
	}
	// Note: consider making ConversationID optional (*uint64 + omitempty) at some point.
	if m.Conversation.ProviderChatID == "" {
		ve.add("conversation.provider_chat_id", "required")
	}
	if m.Message.ProviderMessageID == "" {
		ve.add("message.provider_message_id", "required")
	}
	if _, ok := allowedMessageKinds[m.Kind]; !ok {
		ve.add("kind", fmt.Sprintf("unsupported: %q", m.Kind))
	}
	if m.ReceivedAt.IsZero() {
		ve.add("received_at", "required (non-zero)")
	}
	// Body rules (mirror your receipt checks lightly)
	if m.Body.TextPreview != "" && !m.Body.HasText {
		ve.add("body.has_text", "must be true when text_preview is set")
	}
	if len(m.Body.TextPreview) > 512 {
		ve.add("body.text_preview", "must be ≤ 512 bytes")
	}

	// Attachments
	seenIDs := make(map[string]struct{}, len(m.Attachments))
	for i, a := range m.Attachments {
		if a.ProviderRef.MessageID != m.Message.ProviderMessageID {
			ve.add(fmt.Sprintf("attachments[%d].provider_ref.message_id", i), "must equal message.provider_message_id")
		}
		if err := a.Validate(); err != nil {
			ve.add(fmt.Sprintf("attachments[%d]", i), err.Error())
		}
		if _, dup := seenIDs[a.ID]; dup {
			ve.add(fmt.Sprintf("attachments[%d].id", i), "duplicate id")
		}
		seenIDs[a.ID] = struct{}{}
	}

	if len(ve.Issues) > 0 {
		return ve
	}
	return nil
}

func DeterministicAttachmentID(providerMsgID string, idx int) string {
	return fmt.Sprintf("%s#%d", providerMsgID, idx)
}

// Convenience constructors (optional but nice)

func NewInboundText(
	t TenantRef, p ProviderRef, c ConversationKey,
	providerMsgID, chatID string,
	atProv, atRecv time.Time,
	textPreview, textFP string,
) ChatInboundV1 {
	return ChatInboundV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		Message:      MessageKey{ProviderMessageID: providerMsgID},
		Kind:         "text",
		AtProvider:   atProv,
		ReceivedAt:   atRecv,
		Body:         NewBodyDescriptorText(textPreview, textFP),
	}
}

func NewInboundMediaStub(
	kind string, // "image","audio","video","document","sticker","location","contact"
	t TenantRef, p ProviderRef, c ConversationKey,
	providerMsgID, chatID string,
	atProv, atRecv time.Time,
	captionPreview, captionFP string,
	placeholders []AttachmentDescriptor,
) ChatInboundV1 {
	// Ensure deterministic fields
	for i := range placeholders {
		placeholders[i].ID = DeterministicAttachmentID(providerMsgID, placeholders[i].ProviderRef.AttachmentIndex)
		placeholders[i].State = AttPending
		placeholders[i].ProviderRef.MessageID = providerMsgID
		placeholders[i].ProviderRef.ChatID = chatID
	}
	msg := ChatInboundV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		Message:      MessageKey{ProviderMessageID: providerMsgID},
		Kind:         kind,
		AtProvider:   atProv,
		ReceivedAt:   atRecv,
		Body:         NewBodyDescriptorText(captionPreview, captionFP, kind),
		Attachments:  placeholders,
	}
	msg.Normalize()
	return msg
}
