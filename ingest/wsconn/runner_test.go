package wsconn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestBackoffBounds(t *testing.T) {
	lo, hi := 100*time.Millisecond, 2*time.Second
	for attempt := range 40 {
		for range 50 {
			d := Backoff(attempt, lo, hi)
			ceil := min(lo<<min(attempt, 30), hi)
			if d < lo || d > max(ceil, lo) {
				t.Fatalf("attempt %d: %v outside [%v,%v]", attempt, d, lo, ceil)
			}
		}
	}
}

// The server answers each subscription with one frame and then goes silent.
// The watchdog must force a reconnect, and each new session must resubscribe.
func TestSilenceForcesReconnectAndResubscribe(t *testing.T) {
	var sessions atomic.Int32
	var mu sync.Mutex
	var subs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		sessions.Add(1)
		_, msg, err := c.Read(r.Context())
		if err != nil {
			return
		}
		mu.Lock()
		subs = append(subs, string(msg))
		mu.Unlock()
		c.Write(r.Context(), websocket.MessageText, []byte(`{"hello":1}`))
		<-r.Context().Done() // silence
	}))
	defer srv.Close()

	var frames, connects atomic.Int32
	reasons := make(chan string, 10)
	r := New(Config{
		Name:       "test",
		URL:        "ws" + strings.TrimPrefix(srv.URL, "http"),
		Subscribe:  func() [][]byte { return [][]byte{[]byte(`{"sub":1}`)} },
		OnConnect:  func() { connects.Add(1) },
		OnFrame:    func([]byte, time.Time) { frames.Add(1) },
		OnRestart:  func(reason string) { reasons <- reason },
		Silence:    300 * time.Millisecond,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go r.Run(ctx)

	for i := range 2 {
		select {
		case reason := <-reasons:
			if reason != "silence" {
				t.Fatalf("restart %d reason %q, want silence", i, reason)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for reconnects")
		}
	}
	cancel()
	if connects.Load() < 2 || frames.Load() < 2 {
		t.Fatalf("connects=%d frames=%d", connects.Load(), frames.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(subs) < 2 || subs[0] != `{"sub":1}` || subs[1] != `{"sub":1}` {
		t.Fatalf("subscriptions seen by server: %v", subs)
	}
}

func TestDialFailureBacksOff(t *testing.T) {
	reasons := make(chan string, 10)
	r := New(Config{
		Name:       "test",
		URL:        "ws://127.0.0.1:1", // nothing listens here
		Subscribe:  func() [][]byte { return nil },
		OnFrame:    func([]byte, time.Time) {},
		OnRestart:  func(reason string) { reasons <- reason },
		MinBackoff: 5 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go r.Run(ctx)
	for range 3 {
		select {
		case reason := <-reasons:
			if reason != "dial" {
				t.Fatalf("reason %q", reason)
			}
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
}
