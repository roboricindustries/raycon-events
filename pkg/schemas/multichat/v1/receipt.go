package multichat

import (
	"errors"
	"fmt"
	"time"
)

type ReceiptStatus string

const (
	ReceiptAccepted  ReceiptStatus = "accepted"
	ReceiptRejected  ReceiptStatus = "rejected"
	ReceiptSent      ReceiptStatus = "sent"
	ReceiptDelivered ReceiptStatus = "delivered"
	ReceiptRead      ReceiptStatus = "read"
	ReceiptFailed    ReceiptStatus = "failed"
)

type ReceiptError struct {
	Code    string `json:"code"`              // "401","400","media_unsupported",...
	Details string `json:"details,omitempty"` // provider/raw reason if safe
}

type ChatReceiptV1 struct {
	Tenant       TenantRef       `json:"tenant"`
	Provider     ProviderRef     `json:"provider"`
	Conversation ConversationKey `json:"conversation"`

	// Correlation — either (or both) may be present:
	OutboundID string     `json:"outbound_id,omitempty"` // our local id (from send job)
	Message    MessageKey `json:"message,omitempty"`     // provider message id

	// Lifecycle status:
	// Processor publishes: "accepted" | "rejected"
	// Receiver publishes:  "sent" | "delivered" | "read" | "failed"
	Status ReceiptStatus `json:"status"` // enum as above

	// Message body
	Body BodyDescriptor `json:"body,omitempty"`

	// Timestamps:
	// - AtProcessor: when Processor got sync ack from provider (accepted/rejected)
	// - AtProvider:  provider-reported time, if present (sent/delivered/read/failed)
	AtProcessor *time.Time `json:"at_processor,omitempty"`
	AtProvider  *time.Time `json:"at_provider,omitempty"`

	// Optional echo of the request (lightweight; Hub already knows full text)
	// Useful if you want to show content for accepted quickly without DB hop
	Request struct {
		Kind               string `json:"kind,omitempty"` // "text"
		TextPreview        string `json:"text_preview,omitempty"`
		ContentFingerprint string `json:"content_fingerprint,omitempty"` // sha256 of full content
	} `json:"request"`

	// Error only for "rejected" or "failed"
	Error *ReceiptError `json:"error,omitempty"`
}

// ---------------- Validation ----------------

type ValidationIssue struct{ Field, Reason string }

type ValidationError struct{ Issues []ValidationIssue }

var ErrInvalidContract = errors.New("invalid contract")

func (e *ValidationError) Error() string { return ErrInvalidContract.Error() }
func (e *ValidationError) add(f, r string) {
	e.Issues = append(e.Issues, ValidationIssue{Field: f, Reason: r})
}
func (e *ValidationError) Is(target error) bool { return target == ErrInvalidContract }

// Basic structural checks shared by all statuses
func (r *ChatReceiptV1) validateCommon(ve *ValidationError) {
	// Tenant: at least one id should be set (company or counterparty)
	if r.Tenant.CompanyID <= 0 && r.Tenant.CounterpartyID <= 0 {
		ve.add("tenant", "one of company_id or counterparty_id must be > 0")
	}
	// Provider
	if r.Provider.Provider == "" {
		ve.add("provider.provider", "required")
	}
	if r.Provider.InstanceID == "" {
		ve.add("provider.instance_id", "required")
	}
	// Conversation
	if r.Conversation.ProviderChatID == "" {
		ve.add("conversation.provider_chat_id", "required")
	}
	// Body rules (generic)
	if r.Body.TextPreview != "" && !r.Body.HasText {
		ve.add("body.has_text", "must be true when text_preview is set")
	}
	if byteLen(r.Body.TextPreview) > 512 {
		ve.add("body.text_preview", "must be ≤ 512 bytes")
	}
	if r.Body.Fingerprint != "" && !r.Body.HasText {
		ve.add("body.has_text", "must be true when fingerprint is set")
	}
	// Request (preview must be ≤512 bytes)
	if byteLen(r.Request.TextPreview) > 512 {
		ve.add("request.text_preview", "must be ≤ 512 bytes")
	}
}

func (r *ChatReceiptV1) Validate() error {
	ve := &ValidationError{}

	if r.Status == "" {
		ve.add("status", "required")
	}

	r.validateCommon(ve)

	switch ReceiptStatus(r.Status) {
	case ReceiptAccepted:
		// Processor-only
		if r.OutboundID == "" {
			ve.add("outbound_id", "required for accepted")
		}
		if r.AtProcessor == nil {
			ve.add("at_processor", "required for accepted")
		}
		if r.Error != nil {
			ve.add("error", "must be empty for accepted")
		}
		// Message.ProviderMessageID MAY be empty (unknown at accept time)
		// Request echo (preview/fingerprint) is allowed, Body should be empty
		if r.Body.HasText || r.Body.TextPreview != "" || r.Body.Fingerprint != "" || len(r.Body.MediaKinds) > 0 {
			ve.add("body", "omit for accepted (use request.* for echo)")
		}

	case ReceiptRejected:
		// Processor-only
		if r.OutboundID == "" {
			ve.add("outbound_id", "required for rejected")
		}
		if r.AtProcessor == nil {
			ve.add("at_processor", "required for rejected")
		}
		if r.Error == nil || r.Error.Code == "" {
			ve.add("error.code", "required for rejected")
		}
		if r.Message.ProviderMessageID != "" {
			ve.add("message.provider_message_id", "omit for rejected")
		}
		if r.Request.Kind != "" || r.Request.TextPreview != "" || r.Request.ContentFingerprint != "" {
			ve.add("request", "omit for rejected")
		}
		if r.Body.HasText || r.Body.TextPreview != "" || r.Body.Fingerprint != "" || len(r.Body.MediaKinds) > 0 {
			ve.add("body", "omit for rejected")
		}

	case ReceiptSent, ReceiptDelivered, ReceiptRead:
		// Receiver-only: requires provider id & provider time
		if r.Message.ProviderMessageID == "" {
			ve.add("message.provider_message_id", "required for transport status")
		}
		if r.AtProvider == nil {
			ve.add("at_provider", "required for transport status")
		}
		if r.Error != nil {
			ve.add("error", "must be empty for transport status")
		}
		// Request must be empty
		if r.Request.Kind != "" || r.Request.TextPreview != "" || r.Request.ContentFingerprint != "" {
			ve.add("request", "omit for transport status")
		}
		// Body is OPTIONAL; if body.HasText then Fingerprint is strongly recommended (not enforced)

	case ReceiptFailed:
		// Receiver-only: allow either provider id or outbound id (ambiguous resolution)
		if r.Message.ProviderMessageID == "" && r.OutboundID == "" {
			ve.add("message/outbound", "one of provider_message_id or outbound_id is required for failed")
		}
		if r.AtProvider == nil {
			ve.add("at_provider", "required for failed")
		}
		if r.Error == nil || r.Error.Code == "" {
			ve.add("error.code", "required for failed")
		}
		// Request must be empty
		if r.Request.Kind != "" || r.Request.TextPreview != "" || r.Request.ContentFingerprint != "" {
			ve.add("request", "omit for failed")
		}
		// Body optional

	default:
		ve.add("status", "unknown")
	}

	if len(ve.Issues) > 0 {
		return ve
	}
	return nil
}

// ---------------- Constructors (ergonomic & safe) ----------------

// NewAcceptedReceipt builds a Processor "accepted" receipt.
// providerMsgID is optional; use empty string if unknown at accept time.
// preview is trimmed to ≤512 bytes; fp is a sha256 of the full content (optional).
func NewAcceptedReceipt(
	t TenantRef, p ProviderRef, c ConversationKey,
	outboundID string,
	providerMsgID string,
	at time.Time,
	preview string,
	fp string,
) ChatReceiptV1 {
	r := ChatReceiptV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		OutboundID:   outboundID,
		Status:       ReceiptAccepted,
		AtProcessor:  &at,
	}
	if providerMsgID != "" {
		r.Message.ProviderMessageID = providerMsgID
	}
	if preview != "" || fp != "" {
		r.Request.Kind = "text"
		r.Request.TextPreview = truncateUTF8ToBytes(preview, 512)
		r.Request.ContentFingerprint = fp
	}
	return r
}

// NewRejectedReceipt builds a Processor "rejected" receipt.
func NewRejectedReceipt(
	t TenantRef, p ProviderRef, c ConversationKey,
	outboundID string,
	at time.Time,
	code, details string,
) ChatReceiptV1 {
	r := ChatReceiptV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		OutboundID:   outboundID,
		Status:       ReceiptRejected,
		AtProcessor:  &at,
		Error:        &ReceiptError{Code: code, Details: details},
	}
	return r
}

// NewTransportReceipt builds a Receiver transport receipt: sent|delivered|read.
func NewTransportReceipt(
	t TenantRef, p ProviderRef, c ConversationKey,
	status ReceiptStatus, // one of ReceiptSent/Delivered/Read
	providerMsgID string, // required
	atProvider time.Time, // required
	body BodyDescriptor, // optional; use NewBodyDescriptorText for text
) (ChatReceiptV1, error) {
	if status != ReceiptSent && status != ReceiptDelivered && status != ReceiptRead {
		return ChatReceiptV1{}, fmt.Errorf("invalid status for transport receipt: %q", status)
	}
	r := ChatReceiptV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		Status:       status,
		AtProvider:   &atProvider,
		Body:         body,
	}
	r.Message.ProviderMessageID = providerMsgID
	return r, nil
}

// NewFailedReceipt builds a Receiver "failed" receipt.
// Provide at least providerMsgID or outboundID (for ambiguous cases).
func NewFailedReceipt(
	t TenantRef, p ProviderRef, c ConversationKey,
	atProvider time.Time,
	code, details string,
	providerMsgID string,
	outboundID string,
	body BodyDescriptor, // optional
) ChatReceiptV1 {
	r := ChatReceiptV1{
		Tenant:       t,
		Provider:     p,
		Conversation: c,
		Status:       ReceiptFailed,
		AtProvider:   &atProvider,
		Error:        &ReceiptError{Code: code, Details: details},
		Body:         body,
	}
	if providerMsgID != "" {
		r.Message.ProviderMessageID = providerMsgID
	}
	if outboundID != "" {
		r.OutboundID = outboundID
	}
	return r
}

// NewBodyDescriptorText builds a text body descriptor with a UTF-8 safe preview (≤512 bytes).
// fingerprint is the canonical sha256 of the FULL text (recommended).
func NewBodyDescriptorText(preview string, fingerprint string, mediaKinds ...string) BodyDescriptor {
	return BodyDescriptor{
		HasText:     true,
		TextPreview: truncateUTF8ToBytes(preview, 512),
		MediaKinds:  mediaKinds,
		Fingerprint: fingerprint,
	}
}

// ---------------- Small helpers ----------------

// truncateUTF8ToBytes trims a string to at most n bytes without cutting a rune.
func truncateUTF8ToBytes(s string, n int) string {
	if n <= 0 || len(s) == 0 {
		return ""
	}
	if byteLen(s) <= n {
		return s
	}
	// walk back to a rune boundary
	b := []byte(s)
	i := n
	if i > len(b) {
		i = len(b)
	}
	for i > 0 && (b[i-1]&0xC0) == 0x80 {
		i--
	}
	return string(b[:i])
}

// byteLen returns the byte length (not rune count).
func byteLen(s string) int { return len(s) }

// Ensure constructors always produce valid objects in dev/test.
func (r ChatReceiptV1) MustValid() ChatReceiptV1 {
	if err := r.Validate(); err != nil {
		panic(err)
	}
	return r
}

// Optional: quick boolean helpers
func (r ChatReceiptV1) IsProcessorStatus() bool {
	return r.Status == ReceiptAccepted || r.Status == ReceiptRejected
}
func (r ChatReceiptV1) IsReceiverStatus() bool {
	return r.Status == ReceiptSent || r.Status == ReceiptDelivered || r.Status == ReceiptRead || r.Status == ReceiptFailed
}
