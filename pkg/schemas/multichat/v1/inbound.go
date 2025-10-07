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

type ProviderMediaRefV1 struct {
	RefVer           int               `json:"ref_ver"`                  // =1
	URL              string            `json:"url,omitempty"`            // ephemeral direct link if present
	URLExpiresAtUnix *int64            `json:"url_expires_at,omitempty"` // optional, if known
	MediaID          string            `json:"media_id,omitempty"`       // durable provider handle, if any
	Headers          map[string]string `json:"headers,omitempty"`        // rarely needed; auth
}

type AttachmentDescriptor struct {
	ID              string          `json:"id"`    // deterministic: <provider_msg_id>#<idx>
	Kind            string          `json:"kind"`  // "image","audio","video","document","sticker","location","contact"
	State           AttachmentState `json:"state"` // always "pending" in the receiver for now
	AttachmentIndex int64           `json:"attachment_index"`
	Filename        string          `json:"filename,omitempty"`
	Mime            string          `json:"mime,omitempty"`
	SizeBytes       *int64          `json:"size_bytes,omitempty"`
	// What the future downloader needs to call provider’s media API:
	ProviderRef  ProviderMediaRefV1 `json:"provider_ref"`
	ProviderMeta map[string]any     `json:"provider_meta,omitempty"` // harmless raw hints (e.g., GA type)
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

func (a AttachmentDescriptor) Validate(providerMsgID string) error {
	ve := &ValidationError{}

	if a.AttachmentIndex < 0 {
		ve.add("index", "must be >= 0")
	}

	// Deterministic ID enforcement
	want := DeterministicAttachmentID(providerMsgID, a.AttachmentIndex)
	if a.ID != want {
		ve.add("id", fmt.Sprintf("must equal %q", want))
	}

	if _, ok := allowedAttachmentKinds[a.Kind]; !ok {
		ve.add("kind", fmt.Sprintf("unsupported: %q", a.Kind))
	}

	switch a.State {
	case AttPending, AttAvail, AttFailed:
	default:
		ve.add("state", fmt.Sprintf("unsupported: %q", a.State))
	}

	// ProviderRef sanity
	if a.ProviderRef.RefVer != 1 {
		ve.add("provider_ref.ref_ver", "must be 1")
	}
	if a.ProviderRef.URL == "" && a.ProviderRef.MediaID == "" {
		ve.add("provider_ref", "either url or media_id must be present")
	}

	// Size sanity
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
		return m.Attachments[i].AttachmentIndex < m.Attachments[j].AttachmentIndex
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
	seenIdx := make(map[int64]struct{}, len(m.Attachments))
	for i, a := range m.Attachments {
		if err := a.Validate(m.Message.ProviderMessageID); err != nil {
			ve.add(fmt.Sprintf("attachments[%d]", i), err.Error())
		}
		if _, dup := seenIDs[a.ID]; dup {
			ve.add(fmt.Sprintf("attachments[%d].id", i), "duplicate id")
		}
		seenIDs[a.ID] = struct{}{}
		if _, dup := seenIdx[a.AttachmentIndex]; dup {
			ve.add(fmt.Sprintf("attachments[%d].index", i), "duplicate index")
		}
		seenIdx[a.AttachmentIndex] = struct{}{}
	}

	if len(ve.Issues) > 0 {
		return ve
	}
	return nil
}

func DeterministicAttachmentID(providerMsgID string, idx int64) string {
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
		placeholders[i].ID = DeterministicAttachmentID(providerMsgID, placeholders[i].AttachmentIndex)
		placeholders[i].State = AttPending
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
