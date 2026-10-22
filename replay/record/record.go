// Package record writes raw WebSocket frames to hourly zstd-compressed NDJSON
// files and reads them back. One line per frame:
//
//	{"t":<recv unix nanos>,"v":"<venue>","f":<raw frame JSON>}
//
// Recording raw frames, not normalised events, means replay exercises the
// same parsers, gap tracker and book checksums as the live path.
package record

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/klauspost/compress/zstd"
)

type Line struct {
	T int64           `json:"t"`
	V string          `json:"v"`
	F json.RawMessage `json:"f"`
}

// Recorder appends frames for one venue. Write never blocks the caller; if the
// disk falls behind, frames are dropped and counted.
type Recorder struct {
	dir, venue string
	ch         chan Line
	done       chan struct{}
	OnDrop     func()
}

func NewRecorder(dir, venue string) (*Recorder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &Recorder{dir: dir, venue: venue, ch: make(chan Line, 65536), done: make(chan struct{}), OnDrop: func() {}}
	go r.loop()
	return r, nil
}

func (r *Recorder) Write(frame []byte, recv time.Time) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, frame); err != nil {
		return // not JSON; nothing a parser could use either
	}
	select {
	case r.ch <- Line{T: recv.UnixNano(), V: r.venue, F: buf.Bytes()}:
	default:
		r.OnDrop()
	}
}

// Close flushes and closes the current file.
func (r *Recorder) Close() {
	close(r.ch)
	<-r.done
}

func (r *Recorder) loop() {
	defer close(r.done)
	var (
		f    *os.File
		zw   *zstd.Encoder
		bw   *bufio.Writer
		hour string
	)
	closeFile := func() {
		if f == nil {
			return
		}
		bw.Flush()
		zw.Close()
		f.Close()
		f = nil
	}
	defer closeFile()
	flush := time.NewTicker(time.Second)
	defer flush.Stop()
	for {
		select {
		case l, ok := <-r.ch:
			if !ok {
				return
			}
			h := time.Unix(0, l.T).UTC().Format("2006010215")
			if h != hour || f == nil {
				closeFile()
				name := filepath.Join(r.dir, fmt.Sprintf("%s-%s.ndjson.zst", r.venue, h))
				var err error
				f, err = os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
				if err != nil {
					slog.Error("recorder open", "file", name, "err", err)
					continue
				}
				// Appending a new zstd frame to an existing file is valid; readers see one stream.
				zw, _ = zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedDefault))
				bw = bufio.NewWriterSize(zw, 1<<20)
				hour = h
			}
			bw.WriteString(`{"t":`)
			bw.WriteString(strconv.FormatInt(l.T, 10))
			bw.WriteString(`,"v":"`)
			bw.WriteString(l.V)
			bw.WriteString(`","f":`)
			bw.Write(l.F)
			bw.WriteString("}\n")
		case <-flush.C:
			if f != nil {
				bw.Flush()
				zw.Flush()
			}
		}
	}
}

// Reader iterates lines from one recording file.
type Reader struct {
	f  *os.File
	zr *zstd.Decoder
	sc *bufio.Scanner
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	zr, err := zstd.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	return &Reader{f: f, zr: zr, sc: sc}, nil
}

// Next returns the next line, or io.EOF.
func (r *Reader) Next() (Line, error) {
	for r.sc.Scan() {
		var l Line
		if err := json.Unmarshal(r.sc.Bytes(), &l); err != nil {
			return Line{}, fmt.Errorf("record: bad line: %w", err)
		}
		return l, nil
	}
	if err := r.sc.Err(); err != nil {
		// A file still being written ends mid-frame; treat as end of data.
		if err == io.ErrUnexpectedEOF {
			return Line{}, io.EOF
		}
		return Line{}, err
	}
	return Line{}, io.EOF
}

func (r *Reader) Close() {
	r.zr.Close()
	r.f.Close()
}
