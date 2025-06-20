package common

type Envelope struct {
	Meta Meta `json:"meta"`
	Data any  `json:"data"`
}
