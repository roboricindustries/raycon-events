package common

type Envelope struct {
	Meta Meta `json:"meta"`
	Data any  `json:"data"`
}

type GenericEnvelope[T any] struct {
	Meta Meta `json:"meta"`
	Data T    `json:"data"`
}
