package mqtt

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkTrimTwiceTheTopicsKept(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		o := defaults()
		s := newState("home", &o)
		for i := range 2 * s.opts.MaxTopics {
			s.receive(fmt.Sprintf("a/%d/%d", i%50, i), []byte("1"), false, t0.Add(time.Duration(i)))
		}
		b.StartTimer()
		s.trim()
	}
}
