package kraken

import (
	"bufio"
	"os"
	"testing"
	"time"
)

// The fixture is 60 consecutive live BTC/USD book frames (1 snapshot, 59
// updates) recorded on 2026-10-22. Every venue checksum must match.
func TestChecksumMatchesLiveFrames(t *testing.T) {
	f, err := os.Open("testdata/book_btcusd.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	feed := NewFeed("", []string{"BTC/USD"}, 10)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var checked int
	for sc.Scan() {
		evs, err := feed.Parse(sc.Bytes(), time.Now())
		if err != nil {
			t.Fatalf("frame %d: %v", checked, err)
		}
		for _, ev := range evs {
			if ev.BookMismatch != nil {
				t.Fatalf("frame %d: checksum mismatch expected=%d computed=%d", checked, ev.BookMismatch.Expected, ev.BookMismatch.Computed)
			}
			if ev.Book != nil && ev.Book.Checksum != 0 {
				if !ev.Book.ChecksumOk {
					t.Fatalf("frame %d: checksum not ok", checked)
				}
				checked++
			}
		}
	}
	if checked < 59 {
		t.Fatalf("validated %d checksums, want at least 59", checked)
	}
}

func TestCorruptedUpdateTriggersResync(t *testing.T) {
	feed := NewFeed("", []string{"BTC/USD"}, 10)
	snap := `{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":100.0,"qty":1.00000000}],"asks":[{"price":101.0,"qty":2.00000000}],"timestamp":"2026-10-22T00:00:00Z"}]}`
	if _, err := feed.Parse([]byte(snap), time.Now()); err != nil {
		t.Fatal(err)
	}
	// Wrong checksum on purpose.
	upd := `{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":100.5,"qty":0.50000000}],"asks":[],"checksum":12345,"timestamp":"2026-10-22T00:00:01Z"}]}`
	evs, err := feed.Parse([]byte(upd), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var mismatch bool
	for _, ev := range evs {
		if ev.BookMismatch != nil {
			mismatch = true
			if len(ev.BookMismatch.Messages) != 2 {
				t.Fatalf("want unsubscribe+subscribe, got %d frames", len(ev.BookMismatch.Messages))
			}
		}
	}
	if !mismatch {
		t.Fatal("expected a BookMismatch event")
	}
	// Updates are ignored until a fresh snapshot arrives, which reports BookResynced.
	if evs, _ := feed.Parse([]byte(upd), time.Now()); len(evs) != 0 {
		t.Fatalf("update during resync produced %d events", len(evs))
	}
	evs, _ = feed.Parse([]byte(snap), time.Now())
	var resynced bool
	for _, ev := range evs {
		if ev.BookResynced == "BTC-USD" {
			resynced = true
		}
	}
	if !resynced {
		t.Fatal("expected BookResynced after fresh snapshot")
	}
}

func TestChecksumString(t *testing.T) {
	// Example from Kraken's docs: 45285.2 -> "452852", 0.00100000 -> "100000".
	b := NewBook(1, 8, 10)
	if err := b.Apply([]Level{{"45285.2", "0.00100000"}}, false); err != nil {
		t.Fatal(err)
	}
	p, _ := ParseScaled("45285.2", 1)
	q, _ := ParseScaled("0.00100000", 8)
	if p != 452852 || q != 100000 {
		t.Fatalf("scaled = %d, %d", p, q)
	}
}

func TestParseScaled(t *testing.T) {
	cases := []struct {
		in   string
		prec int
		want int64
		err  bool
	}{
		{"82200.0", 1, 822000, false},
		{"82200", 1, 822000, false},
		{"0.00006120", 8, 6120, false},
		{"0.0000612", 8, 6120, false},
		{"6.12e-05", 8, 6120, false},
		{"1E-8", 8, 1, false},
		{"0.123456789", 8, 0, true},
		{"1.50", 1, 15, false},
		{"abc", 2, 0, true},
	}
	for _, c := range cases {
		got, err := ParseScaled(c.in, c.prec)
		if (err != nil) != c.err || (!c.err && got != c.want) {
			t.Errorf("ParseScaled(%q,%d) = %d, %v; want %d, err=%v", c.in, c.prec, got, err, c.want, c.err)
		}
	}
}

func TestFormat(t *testing.T) {
	cases := map[string]struct {
		v    int64
		prec int
	}{
		"0.00006120": {6120, 8},
		"82200.0":    {822000, 1},
		"5":          {5, 0},
		"0.05":       {5, 2},
	}
	for want, c := range cases {
		if got := Format(c.v, c.prec); got != want {
			t.Errorf("Format(%d,%d) = %q, want %q", c.v, c.prec, got, want)
		}
	}
}

func TestTruncateKeepsBestLevels(t *testing.T) {
	b := NewBook(0, 0, 2)
	b.Apply([]Level{{"1", "1"}, {"2", "1"}, {"3", "1"}}, true)
	b.Apply([]Level{{"4", "1"}, {"5", "1"}, {"6", "1"}}, false)
	b.Truncate()
	if _, ok := b.bids[1]; ok || len(b.bids) != 2 {
		t.Fatalf("bids after truncate: %v", b.bids)
	}
	if _, ok := b.asks[6]; ok || len(b.asks) != 2 {
		t.Fatalf("asks after truncate: %v", b.asks)
	}
}
