// Command replay feeds recorded raw frames back through the live parsers and
// pipeline into Kafka at N× the original speed (or as fast as possible).
//
//	replay --speed 10 ~/.tidemark-data/recordings/*-2026102207.ndjson.zst
//	replay --speed 0 --raw --loop 20 --topic-suffix .bench <files>   # throughput
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/ingest/pipeline"
	"tidemark/ingest/sink"
	"tidemark/ingest/venue"
	"tidemark/ingest/venue/coinbase"
	"tidemark/ingest/venue/kraken"
	"tidemark/replay/inject"
	"tidemark/replay/record"
)

func main() {
	var (
		brokers     = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		registry    = flag.String("registry", "http://localhost:18081", "schema registry URL")
		metricsAddr = flag.String("metrics-addr", ":9103", "Prometheus listen address; empty disables")
		speed       = flag.Float64("speed", 1, "replay speed multiplier; 0 = as fast as possible")
		raw         = flag.Bool("raw", false, "skip gap tracking and dedup (throughput benchmarks)")
		loop        = flag.Int("loop", 1, "replay the files this many times (use with --raw)")
		dryRun      = flag.Bool("dry-run", false, "do not produce to Kafka")
		topicSuffix = flag.String("topic-suffix", "", "suffix appended to topic names")
		depth       = flag.Int("kraken-book-depth", 10, "must match the depth used when recording")
		perPattern  = flag.Int("inject-per-pattern", 0, "inject this many labelled patterns of each kind (0 = off)")
		injectSeed  = flag.Uint64("inject-seed", 42, "seed for the injection plan")
		warmup      = flag.Duration("inject-warmup", 10*time.Minute, "no injections before this much stream time (detector baselines)")
		injectGap   = flag.Duration("inject-gap", 90*time.Second, "minimum spacing between injections")
		accounts    = flag.Int("accounts", 2000, "synthetic account pool size (wash-trade evaluation)")
	)
	flag.Parse()
	files := flag.Args()
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "usage: replay [flags] <recording.ndjson.zst>...")
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", m.Handler())
			_ = (&http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
		}()
	}

	var out sink.Sink = countOnly{}
	if !*dryRun {
		k, err := sink.NewKafka(ctx, sink.KafkaConfig{
			Brokers:     strings.Split(*brokers, ","),
			RegistryURL: *registry,
			Topics:      sink.DefaultTopics(*topicSuffix),
			ClientID:    "tidemark-replay",
		}, m)
		if err != nil {
			slog.Error("kafka", "err", err)
			os.Exit(1)
		}
		out = k
	}
	var injector *inject.Injector
	if *perPattern > 0 {
		if *dryRun {
			slog.Error("--inject-per-pattern needs Kafka for labels; drop --dry-run")
			os.Exit(2)
		}
		t0, t1, err := span(files)
		if err != nil {
			slog.Error("read", "err", err)
			os.Exit(1)
		}
		plan := inject.Plan(*injectSeed, time.Unix(0, t0), time.Unix(0, t1), *warmup, *injectGap, *perPattern,
			[]string{"BTC-USD", "ETH-USD", "SOL-USD"})
		injector = inject.New(out, out.(*sink.Kafka), plan, *accounts)
		out = injector
		slog.Info("injection plan", "injections", len(plan), "from", time.Unix(0, t0).UTC(), "to", time.Unix(0, t1).UTC())
	}
	counted := &counter{Sink: out}

	byVenue := map[string][]string{}
	for _, f := range files {
		v, _, _ := strings.Cut(filepath.Base(f), "-")
		byVenue[v] = append(byVenue[v], f)
	}
	t0, err := earliest(files)
	if err != nil {
		slog.Error("read", "err", err)
		os.Exit(1)
	}

	start := time.Now()
	var wg sync.WaitGroup
	var frames atomic.Int64
	for v, fs := range byVenue {
		slices.Sort(fs)
		var feed venue.Feed
		switch v {
		case "coinbase":
			feed = coinbase.NewFeed("", nil)
		case "kraken":
			feed = kraken.NewFeed("", nil, *depth)
		default:
			slog.Error("unknown venue in file name", "venue", v)
			os.Exit(2)
		}
		p := pipeline.New(feed, counted, m, pipeline.Options{
			Source:    pb.Source_SOURCE_REPLAY,
			IngestID:  "replay",
			TrackGaps: !*raw,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range *loop {
				clock := start // venues share one timeline; later loops restart it
				if i > 0 {
					clock = time.Now()
				}
				for _, f := range fs {
					n, err := play(ctx, f, p, t0, clock, *speed)
					frames.Add(n)
					if err != nil {
						slog.Error("replay", "file", f, "err", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	fctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := out.Flush(fctx); err != nil {
		slog.Error("flush", "err", err)
	}
	elapsed := time.Since(start)
	out.Close()

	n := counted.trades.Load() + counted.books.Load()
	if injector != nil {
		fmt.Printf("injected %v skipped=%d %v\n", injector.Fired, len(injector.Skipped), injector.Skipped)
	}
	fmt.Printf("replayed frames=%d trades=%d book_updates=%d gaps=%d elapsed=%s records_per_sec=%.0f\n",
		frames.Load(), counted.trades.Load(), counted.books.Load(), counted.gaps.Load(),
		elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}

// play streams one file. With speed > 0 each frame is delayed so that
// (frame time - t0) / speed matches wall-clock time since start.
func play(ctx context.Context, path string, p *pipeline.Pipeline, t0 int64, start time.Time, speed float64) (int64, error) {
	r, err := record.Open(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	var n int64
	for {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if speed > 0 {
			due := start.Add(time.Duration(float64(l.T-t0) / speed))
			if d := time.Until(due); d > 0 {
				select {
				case <-ctx.Done():
					return n, ctx.Err()
				case <-time.After(d):
				}
			}
		}
		p.HandleFrame(l.F, time.Now())
		n++
	}
}

// span returns the first and last frame times across files.
func span(files []string) (int64, int64, error) {
	var lo, hi int64
	for _, f := range files {
		r, err := record.Open(f)
		if err != nil {
			return 0, 0, err
		}
		for {
			l, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.Close()
				return 0, 0, err
			}
			if lo == 0 || l.T < lo {
				lo = l.T
			}
			hi = max(hi, l.T)
		}
		r.Close()
	}
	return lo, hi, nil
}

func earliest(files []string) (int64, error) {
	var t0 int64
	for _, f := range files {
		r, err := record.Open(f)
		if err != nil {
			return 0, err
		}
		l, err := r.Next()
		r.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if err == nil && (t0 == 0 || l.T < t0) {
			t0 = l.T
		}
	}
	return t0, nil
}

type counter struct {
	sink.Sink
	trades, books, gaps atomic.Int64
}

func (c *counter) Trade(t *pb.Trade)     { c.trades.Add(1); c.Sink.Trade(t) }
func (c *counter) Book(b *pb.BookUpdate) { c.books.Add(1); c.Sink.Book(b) }
func (c *counter) Gap(g *pb.GapEvent)    { c.gaps.Add(1); c.Sink.Gap(g) }

type countOnly struct{}

func (countOnly) Trade(*pb.Trade)             {}
func (countOnly) Book(*pb.BookUpdate)         {}
func (countOnly) Gap(*pb.GapEvent)            {}
func (countOnly) Flush(context.Context) error { return nil }
func (countOnly) Close()                      {}
