package verify

import (
	"errors"
	"github.com/fastabc/fastmesh/model"
	"testing"
)

func TestPlanBodySizeBeforeSigningPayload(t *testing.T) {
	for _, n := range []int{model.MaxPlanBodyBytes - 1, model.MaxPlanBodyBytes, model.MaxPlanBodyBytes + 1} {
		payload, err := PlanBodySigningPayload(model.PlanTypeURL, make([]byte, n))
		if n > model.MaxPlanBodyBytes {
			if !errors.Is(err, model.ErrPlanTooLarge) || payload != nil {
				t.Fatalf("oversize: %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
