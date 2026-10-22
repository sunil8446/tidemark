package record

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir, "kraken")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 22, 7, 59, 59, 0, time.UTC)
	frames := []string{`{"a": 1}`, `{"price":82200.0,"qty":0.00006120}`, `{"b":[1, 2]}`}
	for i, f := range frames {
		rec.Write([]byte(f), base.Add(time.Duration(i)*time.Second)) // crosses the hour boundary
	}
	rec.Write([]byte("not json"), base)
	rec.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "kraken-*.ndjson.zst"))
	if len(files) != 2 {
		t.Fatalf("want 2 hourly files, got %v", files)
	}
	var got []Line
	for _, f := range files {
		r, err := Open(f)
		if err != nil {
			t.Fatal(err)
		}
		for {
			l, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, l)
		}
		r.Close()
	}
	if len(got) != 3 {
		t.Fatalf("got %d lines", len(got))
	}
	// Numbers keep their exact text (needed for book checksums); whitespace is compacted.
	if string(got[1].F) != `{"price":82200.0,"qty":0.00006120}` || string(got[0].F) != `{"a":1}` {
		t.Fatalf("frames changed: %s | %s", got[0].F, got[1].F)
	}
	if got[0].V != "kraken" || got[0].T != base.UnixNano() {
		t.Fatalf("line %+v", got[0])
	}
}
