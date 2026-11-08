package discovery

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestCoalesceBoundsContinuousChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCoalescer(50 * time.Millisecond)
		defer c.stop()
		for range 3 {
			c.schedule()
			for range 4 {
				time.Sleep(10 * time.Millisecond)
				c.schedule()
			}
			select {
			case <-c.ch:
				t.Fatal("early push")
			default:
			}
			time.Sleep(10 * time.Millisecond)
			select {
			case <-c.ch:
				c.fired()
			default:
				t.Fatal("continuous changes postponed window")
			}
		}
	})
}
