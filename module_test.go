package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/hooks/auth"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

// closedAddr is a loopback address nothing listens on.
func closedAddr(t *testing.T) string { return freeAddr(t) }

func TestTheFirstSnapshotHoldsTheRetainedMessages(t *testing.T) {
	b := startBroker(t, false, new(auth.AllowHook), nil)
	b.publish(t, "garden/temperature", "14.2", true)
	m := configured(t, "url: mqtt://"+b.addr+"\ntopics: ['#']\ninterval: 1m\n")
	start := time.Now()
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	sink.WaitFor(t, 1)
	if took := time.Since(start); took >= firstWait {
		t.Errorf("the first snapshot took %v, as if the broker never answered", took)
	}
	if !upserts(sink.Sets()[0], "garden/temperature") {
		t.Error("the first snapshot doesn't hold the retained topic")
	}
}

func TestTheFirstSnapshotComesWithoutABroker(t *testing.T) {
	m := configured(t, "url: mqtt://"+closedAddr(t)+"\ntopics: ['#']\ninterval: 1m\n")
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	sink.WaitFor(t, 1)
}

func upserts(cs sdk.ChangeSet, topic string) bool {
	for _, e := range cs.Upserts {
		if e.Attrs["topic"].Str() == topic {
			return true
		}
	}
	return false
}
