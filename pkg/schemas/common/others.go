package common

type Targeting struct {
	// If omitted or empty => target all active subscriptions for the principal.
	AppIDs     []string `json:"app_ids,omitempty"`    // e.g. ["web","ios","android"]
	Transports []string `json:"transports,omitempty"` // e.g. ["webpush","apns","fcm"]
}
