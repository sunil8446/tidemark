// Package wsconn keeps one WebSocket session alive: dial, subscribe, read,
// and on any failure reconnect with capped exponential backoff and full
// jitter. A watchdog forces a reconnect when the venue goes silent.
package wsconn

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type Config struct {
	Name       string
	URL        string
	Subscribe  func() [][]byte // frames sent after every connect
	OnConnect  func()          // called before subscribing; reset per-connection state
	OnFrame    func(frame []byte, recv time.Time)
	OnState    func(connected bool) // optional
	OnRestart  func(reason string)  // optional, called once per reconnect
	Silence    time.Duration        // no frame for this long forces a reconnect
	MinBackoff time.Duration
	MaxBackoff time.Duration
	StableFor  time.Duration // a session that lasts this long resets the backoff
}

type Runner struct {
	cfg    Config
	send   chan []byte
	log    *slog.Logger
	cancel atomic.Pointer[context.CancelFunc] // current session
}

func New(cfg Config) *Runner {
	if cfg.Silence == 0 {
		cfg.Silence = 15 * time.Second
	}
	if cfg.MinBackoff == 0 {
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = time.Minute
	}
	if cfg.StableFor == 0 {
		cfg.StableFor = time.Minute
	}
	if cfg.OnState == nil {
		cfg.OnState = func(bool) {}
	}
	if cfg.OnRestart == nil {
		cfg.OnRestart = func(string) {}
	}
	return &Runner{cfg: cfg, send: make(chan []byte, 64), log: slog.With("venue", cfg.Name)}
}

// Send queues a frame for the current session. Frames queued while
// disconnected are discarded at the next connect, because every session
// resubscribes from scratch anyway.
func (r *Runner) Send(frame []byte) {
	select {
	case r.send <- frame:
	default:
		r.log.Warn("send queue full; dropping frame")
	}
}

// Reconnect ends the current session; Run then reconnects as after any failure.
func (r *Runner) Reconnect() {
	if c := r.cancel.Load(); c != nil {
		(*c)()
	}
}

// Run blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	attempt := 0
	for {
		start := time.Now()
		reason := r.session(ctx)
		r.cfg.OnState(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(start) >= r.cfg.StableFor {
			attempt = 0
		}
		r.cfg.OnRestart(reason)
		d := Backoff(attempt, r.cfg.MinBackoff, r.cfg.MaxBackoff)
		attempt++
		r.log.Warn("session ended; reconnecting", "reason", reason, "after", d.Round(time.Millisecond), "attempt", attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
}

// Backoff is "full jitter": uniform in [min, min(max, min*2^attempt)].
func Backoff(attempt int, lo, hi time.Duration) time.Duration {
	ceil := lo << min(attempt, 30)
	if ceil > hi || ceil <= 0 {
		ceil = hi
	}
	if ceil <= lo {
		return lo
	}
	return lo + rand.N(ceil-lo)
}

func (r *Runner) session(ctx context.Context) (reason string) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.cancel.Store(&cancel)

	dctx, dcancel := context.WithTimeout(sctx, 10*time.Second)
	c, _, err := websocket.Dial(dctx, r.cfg.URL, nil)
	dcancel()
	if err != nil {
		r.log.Warn("dial failed", "err", err)
		return "dial"
	}
	defer c.CloseNow()
	c.SetReadLimit(64 << 20)

	for len(r.send) > 0 { // stale frames from the previous session
		<-r.send
	}
	if r.cfg.OnConnect != nil {
		r.cfg.OnConnect()
	}
	for _, m := range r.cfg.Subscribe() {
		if err := c.Write(sctx, websocket.MessageText, m); err != nil {
			return "subscribe"
		}
	}
	r.cfg.OnState(true)
	r.log.Info("connected", "url", r.cfg.URL)

	var lastFrame atomic.Int64
	lastFrame.Store(time.Now().UnixNano())
	var silent atomic.Bool

	go func() { // writer
		for {
			select {
			case <-sctx.Done():
				return
			case m := <-r.send:
				if err := c.Write(sctx, websocket.MessageText, m); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	go func() { // watchdog
		t := time.NewTicker(r.cfg.Silence / 4)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case now := <-t.C:
				if now.Sub(time.Unix(0, lastFrame.Load())) > r.cfg.Silence {
					silent.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	for {
		_, frame, err := c.Read(sctx)
		if err != nil {
			switch {
			case silent.Load():
				return "silence"
			case ctx.Err() != nil:
				c.Close(websocket.StatusNormalClosure, "shutdown")
				return "shutdown"
			case websocket.CloseStatus(err) != -1:
				return "closed_by_venue"
			case errors.Is(err, context.Canceled):
				return "forced_or_write_error"
			default:
				return "read_error"
			}
		}
		now := time.Now()
		lastFrame.Store(now.UnixNano())
		r.cfg.OnFrame(frame, now)
	}
}
