package notifications

import (
	"encoding/json"
	"fmt"

	"github.com/roboricindustries/raycon-events/pkg/constants"
	"github.com/roboricindustries/raycon-events/pkg/schemas/common"
)

// CP-specific
type WAGreenAPIOptions struct {
	InstanceID uint `json:"instance_id"`
}

type WAGreenAPIMsg struct {
	common.Core
}

func (m WAGreenAPIMsg) GetOptions() (WAGreenAPIOptions, error) {
	var out WAGreenAPIOptions
	b, ok := m.Options[constants.NSWAGreenAPI]
	if !ok {
		return out, fmt.Errorf("options.wa.greenapi missing")
	}
	return out, json.Unmarshal(b, &out)
}

const (
	EventType  = "notification.wa.greenapi.v1"
	Exchange   = "notification.internal"
	RoutingKey = "cp.wa.greenapi"
)

func WAMeta() common.EventMeta {
	return common.EventMeta{
		EventType:  EventType,
		Exchange:   Exchange,
		RoutingKey: RoutingKey,
	}
}
