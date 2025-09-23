package multichat

import "time"

type ReceiptStatus string

const (
	ReceiptAccepted  ReceiptStatus = "accepted"
	ReceiptRejected  ReceiptStatus = "rejected"
	ReceiptSent      ReceiptStatus = "sent"
	ReceiptDelivered ReceiptStatus = "delivered"
	ReceiptRead      ReceiptStatus = "read"
	ReceiptFailed    ReceiptStatus = "failed"
)

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
	Error *struct {
		Code    string `json:"code"`              // "401","400","media_unsupported",...
		Details string `json:"details,omitempty"` // provider/raw reason if safe
	} `json:"error,omitempty"`
}

// --------- validation ----------------

type ValidationIssue struct{ Field, Reason string }
type ValidationError struct{ Issues []ValidationIssue }

func (e *ValidationError) Error() string   { return "invalid contract" }
func (e *ValidationError) add(f, r string) { e.Issues = append(e.Issues, ValidationIssue{f, r}) }

func (r *ChatReceiptV1) Validate() error {
	ve := &ValidationError{}

	if r.Status == "" {
		ve.add("status", "required")
	}
	switch ReceiptStatus(r.Status) {
	case ReceiptAccepted:
		if r.OutboundID == "" {
			ve.add("outbound_id", "required for accepted")
		}
		if r.AtProcessor == nil {
			ve.add("at_processor", "required for accepted")
		}
		if r.Error != nil {
			ve.add("error", "must be empty for accepted")
		}
		// Request echo allowed (optional); Message may be empty if provider id unknown yet
	case ReceiptRejected:
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
		if r.Request.Kind != "" {
			ve.add("request", "omit for rejected")
		}
	case ReceiptSent, ReceiptDelivered, ReceiptRead:
		if r.Message.ProviderMessageID == "" {
			ve.add("message.provider_message_id", "required for transport status")
		}
		if r.AtProvider == nil {
			ve.add("at_provider", "required for transport status")
		}
		if r.Error != nil {
			ve.add("error", "must be empty for transport status")
		}
		// OutboundID is optional (filled when correlation exists)
		// Request must be empty
		if r.Request.Kind != "" {
			ve.add("request", "omit for transport status")
		}
	case ReceiptFailed:
		// Prefer provider id; allow outbound-only if provider id unknown yet
		if r.Message.ProviderMessageID == "" && r.OutboundID == "" {
			ve.add("message/outbound", "one of provider_message_id or outbound_id is required for failed")
		}
		if r.AtProvider == nil {
			ve.add("at_provider", "required for failed")
		}
		if r.Error == nil || r.Error.Code == "" {
			ve.add("error.code", "required for failed")
		}
		if r.Request.Kind != "" {
			ve.add("request", "omit for failed")
		}
	default:
		ve.add("status", "unknown")
	}

	if len(ve.Issues) > 0 {
		return ve
	}
	return nil
}

// Publisher helpers (make invalid states hard to create)
func NewAcceptedReceipt(t TenantRef, p ProviderRef, c ConversationKey, outboundID, providerMsgID string, at time.Time, preview string, fp string) ChatReceiptV1 {
	r := ChatReceiptV1{
		Tenant: t, Provider: p, Conversation: c,
		OutboundID:  outboundID,
		Status:      ReceiptAccepted,
		AtProcessor: &at,
	}
	if providerMsgID != "" {
		r.Message.ProviderMessageID = providerMsgID
	}
	if preview != "" || fp != "" {
		r.Request.Kind = "text"
		if len(preview) > 512 {
			preview = preview[:512]
		}
		r.Request.TextPreview = preview
		r.Request.ContentFingerprint = fp
	}
	return r
}
