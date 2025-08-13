// Code generated from JSON Schema using quicktype. DO NOT EDIT.
// To parse and unparse this JSON data, add this code to your project and do:
//
//    notificationLog, err := UnmarshalNotificationLog(bytes)
//    bytes, err = notificationLog.Marshal()

package main

import (
	"encoding/json"
	"time"
)

func UnmarshalNotificationLog(data []byte) (NotificationLogData, error) {
	var r NotificationLogData
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *NotificationLogData) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type NotificationLogData struct {
	// Original list of requested channels
	Channels []string `json:"channels"`
	// Owner company; null for system/global
	CompanyID *int64 `json:"company_id"`
	// Do not send after this moment
	ExpiresAt *time.Time `json:"expires_at"`
	// Extra metadata for analytics/auditing
	Meta map[string]any `json:"meta"`
	// Semantic code used to pick templates, e.g., crm.lead.converted
	NotificationCode string `json:"notification_code"`
	// Dispatcher → CP instructions (per-channel options)
	Options map[string]any `json:"options"`
	// Variables for template rendering (free-form)
	Params map[string]any `json:"params"`
	// Recipient entity ID
	RecipientID int64 `json:"recipient_id"`
	// DB enum recipient_role_enum value
	RecipientRole string `json:"recipient_role"`
	// Do not send before this moment
	ScheduledAt *time.Time `json:"scheduled_at"`
}
