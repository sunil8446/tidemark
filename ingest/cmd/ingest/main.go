// Command ingest streams live trades and books from Coinbase and Kraken into
// Kafka, detecting and backfilling gaps, and optionally recording raw frames.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/ingest/pipeline"
	"tidemark/ingest/resume"
	"tidemark/ingest/sink"
	"tidemark/ingest/venue"
	"tidemark/ingest/venue/coinbase"
	"tidemark/ingest/venue/kraken"
	"tidemark/ingest/wsconn"
	"tidemark/replay/record"
)

func main() {
	home, _ := os.UserHomeDir()
	host, _ := os.Hostname()
	var (
		brokers     = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		registry    = flag.String("registry", "http://localhost:18081", "schema registry URL")
		metricsAddr = flag.String("metrics-addr", ":9102", "Prometheus listen address")
		cbProducts  = flag.String("coinbase", "BTC-USD,ETH-USD,SOL-USD", "Coinbase products (empty disables)")
		krSymbols   = flag.String("kraken", "BTC/USD,ETH/USD,SOL/USD", "Kraken symbols (empty disables)")
		krDepth     = flag.Int("kraken-book-depth", 10, "Kraken book depth; 0 disables the book channel")
		recordDir   = flag.String("record-dir", filepath.Join(home, ".tidemark-data", "recordings"), "raw frame recordings; empty disables")
		ingestID    = flag.String("ingest-id", host, "identifies this ingestor in records")
		dryRun      = flag.Bool("dry-run", false, "parse and track gaps but do not produce to Kafka")
		silence     = flag.Duration("silence", 15*time.Second, "reconnect when no frame arrives for this long")
		grace       = flag.Duration("backfill-grace", 3*time.Second, "wait before the first REST backfill call")
		topicSuffix = flag.String("topic-suffix", "", "suffix appended to topic names")
		debug       = flag.Bool("debug-endpoints", false, "expose /debug/drop and /debug/reconnect fault injection")
	)
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	mux := http.NewServeMux()
	go func() {
		mux.Handle("/metrics", m.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server", "err", err)
			stop()
		}
	}()

	var out sink.Sink = &discard{}
	if !*dryRun {
		k, err := sink.NewKafka(ctx, sink.KafkaConfig{
			Brokers:      split(*brokers),
			RegistryURL:  *registry,
			Topics:       sink.DefaultTopics(*topicSuffix),
			ClientID:     "tidemark-ingest-" + *ingestID,
			TrackLatency: true,
		}, m)
		if err != nil {
			slog.Error("kafka", "err", err)
			os.Exit(1)
		}
		out = k
	}

	type venueSetup struct {
		feed     venue.Feed
		bf       venue.Backfiller
		products []string
	}
	var setups []venueSetup
	if ps := split(*cbProducts); len(ps) > 0 {
		setups = append(setups, venueSetup{coinbase.NewFeed("", ps), coinbase.NewBackfiller(""), ps})
	}
	if ss := split(*krSymbols); len(ss) > 0 {
		var ps []string
		for _, s := range ss {
			ps = append(ps, kraken.Product(s))
		}
		setups = append(setups, venueSetup{kraken.NewFeed("", ss, *krDepth), kraken.NewBackfiller(""), ps})
	}

	positions := map[string]resume.Position{}
	if !*dryRun {
		rctx, rcancel := context.WithTimeout(ctx, time.Minute)
		var err error
		positions, err = resume.LastSeen(rctx, split(*brokers), sink.DefaultTopics(*topicSuffix).Trades, 5000)
		rcancel()
		if err != nil {
			slog.Error("resume from kafka", "err", err)
			os.Exit(1)
		}
	}

	var recorders []*record.Recorder
	done := make(chan struct{}, len(setups))
	for _, s := range setups {
		name := venue.Name(s.feed.Venue())
		p := pipeline.New(s.feed, out, m, pipeline.Options{
			Source:        pb.Source_SOURCE_WS,
			IngestID:      *ingestID,
			TrackGaps:     true,
			Backfiller:    s.bf,
			BackfillGrace: *grace,
		})
		p.InitMetrics(s.products)
		for _, prod := range s.products {
			if pos, ok := positions[venue.Key(s.feed.Venue(), prod)]; ok {
				p.Tracker().Seed(prod, pos.TradeID, pos.EventTs)
				slog.Info("resuming", "venue", name, "product", prod, "last_trade_id", pos.TradeID)
			}
		}
		m.Connected.WithLabelValues(name)
		for _, reason := range []string{"dial", "subscribe", "silence", "closed_by_venue", "forced_or_write_error", "read_error"} {
			m.Reconnects.WithLabelValues(name, reason)
		}
		onFrame := p.HandleFrame
		if *recordDir != "" {
			rec, err := record.NewRecorder(*recordDir, name)
			if err != nil {
				slog.Error("recorder", "err", err)
				os.Exit(1)
			}
			rec.OnDrop = func() { m.RecorderDropped.WithLabelValues(name).Inc() }
			recorders = append(recorders, rec)
			onFrame = func(f []byte, t time.Time) { rec.Write(f, t); p.HandleFrame(f, t) }
		}
		r := wsconn.New(wsconn.Config{
			Name:      name,
			URL:       s.feed.URL(),
			Subscribe: s.feed.SubscribeMessages,
			OnConnect: p.Reset,
			OnFrame:   onFrame,
			OnState: func(up bool) {
				v := 0.0
				if up {
					v = 1
				}
				m.Connected.WithLabelValues(name).Set(v)
			},
			OnRestart: func(reason string) { m.Reconnects.WithLabelValues(name, reason).Inc() },
			Silence:   *silence,
		})
		p.SetSender(r.Send)
		p.RunBackfill(ctx, 2)
		if *debug {
			registerDebug(mux, name, p, r)
		}
		go func() { r.Run(ctx); done <- struct{}{} }()
	}

	slog.Info("ingest running", "venues", len(setups), "dry_run", *dryRun, "record_dir", *recordDir, "metrics", *metricsAddr)
	<-ctx.Done()
	for range setups {
		<-done
	}
	fctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := out.Flush(fctx); err != nil {
		slog.Error("flush", "err", err)
	}
	out.Close()
	for _, r := range recorders {
		r.Close()
	}
	slog.Info("ingest stopped")
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
