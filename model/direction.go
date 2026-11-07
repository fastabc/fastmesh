package model

// Direction identifies which side of a mesh connection is being observed.
type Direction string

const (
	DirectionInbound  Direction = "inbound"
	DirectionOutbound Direction = "outbound"
)
