package model

import (
	"errors"
	"fmt"
)

var (
	ErrPlanUnavailable    = fmt.Errorf("%w: no fresh plan", ErrDenied)
	ErrServiceUnavailable = fmt.Errorf("%w: service absent", ErrDenied)
	ErrNoEndpoints        = fmt.Errorf("%w: no selectable endpoints", ErrDenied)
)

var ErrDenied = errors.New("mesh: service access denied")
