package model

import "errors"

// Plan limits bound the signed JSON before verification/copying and the complete
// protobuf response including metadata and certificate chains. The envelope
// limit matches gRPC's default receive limit; body headroom is not a substitute
// for checking the full message.
const (
	MaxPlanBodyBytes     = 3 << 20
	MaxPlanEnvelopeBytes = 4 << 20
)

var ErrPlanTooLarge = errors.New("mesh: plan size limit exceeded")
