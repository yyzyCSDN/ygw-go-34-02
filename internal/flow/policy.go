package flow

import "fmt"

// Policy controls the token bucket used for dispatch.
type Policy struct {
	Rate  float64
	Burst int
}

// Validate rejects nonsensical policies.
func (p Policy) Validate() error {
	if p.Rate <= 0 {
		return fmt.Errorf("rate must be positive: %v", p.Rate)
	}
	if p.Burst <= 0 {
		return fmt.Errorf("burst must be positive: %d", p.Burst)
	}
	return nil
}
