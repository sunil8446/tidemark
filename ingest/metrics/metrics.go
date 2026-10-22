// Package metrics holds the ingest Prometheus metrics. Every number the
// project claims about ingestion comes from these series.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry *prometheus.Registry

	Frames           *prometheus.CounterVec   // venue
	ParseErrors      *prometheus.CounterVec   // venue
	Trades           *prometheus.CounterVec   // venue, product, source
	Duplicates       *prometheus.CounterVec   // venue, product
	BookUpdates      *prometheus.CounterVec   // venue, product, checksum_ok
	ChecksumFailures *prometheus.CounterVec   // venue, product
	GapsDetected     *prometheus.CounterVec   // venue, product, kind
	GapsResolved     *prometheus.CounterVec   // venue, product, kind, status
	GapTradesMissing *prometheus.CounterVec   // venue, product
	GapTradesFilled  *prometheus.CounterVec   // venue, product
	GapRecovery      *prometheus.HistogramVec // venue, kind, status
	GapsOpen         *prometheus.GaugeVec     // venue
	Connected        *prometheus.GaugeVec     // venue
	Reconnects       *prometheus.CounterVec   // venue, reason
	Produced         *prometheus.CounterVec   // topic
	ProduceErrors    *prometheus.CounterVec   // topic
	ExchangeToKafka  *prometheus.HistogramVec // venue: exchange event time -> broker ack
	RecorderDropped  *prometheus.CounterVec   // venue
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := factory{r}
	return &Metrics{
		Registry:         r,
		Frames:           f.counter("frames_total", "WebSocket frames received.", "venue"),
		ParseErrors:      f.counter("parse_errors_total", "Frames that failed to parse.", "venue"),
		Trades:           f.counter("trades_total", "Trades handed to the sink.", "venue", "product", "source"),
		Duplicates:       f.counter("duplicate_trades_total", "Trades dropped because the trade_id was already seen.", "venue", "product"),
		BookUpdates:      f.counter("book_updates_total", "Book updates handed to the sink.", "venue", "product", "checksum_ok"),
		ChecksumFailures: f.counter("checksum_failures_total", "Book checksum mismatches.", "venue", "product"),
		GapsDetected:     f.counter("gaps_detected_total", "Gaps opened.", "venue", "product", "kind"),
		GapsResolved:     f.counter("gaps_resolved_total", "Gaps closed, by terminal status.", "venue", "product", "kind", "status"),
		GapTradesMissing: f.counter("gap_trades_missing_total", "Trade ids covered by detected gaps.", "venue", "product"),
		GapTradesFilled:  f.counter("gap_trades_filled_total", "Trade ids inside gaps that were filled (late WS or REST).", "venue", "product"),
		GapRecovery: f.histogram("gap_recovery_seconds", "Time from gap detection to its terminal status.",
			[]float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600}, "venue", "kind", "status"),
		GapsOpen:      f.gauge("gaps_open", "Gaps currently awaiting resolution.", "venue"),
		Connected:     f.gauge("connected", "1 while the WebSocket session is up.", "venue"),
		Reconnects:    f.counter("reconnects_total", "Session restarts, by reason.", "venue", "reason"),
		Produced:      f.counter("produced_total", "Records acknowledged by Kafka.", "topic"),
		ProduceErrors: f.counter("produce_errors_total", "Records that failed to produce.", "topic"),
		ExchangeToKafka: f.histogram("exchange_to_kafka_seconds", "Exchange event timestamp to Kafka ack, live WS trades only. Includes clock skew between the venue and this host.",
			[]float64{.005, .01, .025, .05, .075, .1, .15, .2, .3, .5, .75, 1, 2, 5, 10}, "venue"),
		RecorderDropped: f.counter("recorder_dropped_total", "Frames the recorder could not keep up with.", "venue"),
	}
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

type factory struct{ r *prometheus.Registry }

const ns, sub = "tidemark", "ingest"

func (f factory) counter(name, help string, labels ...string) *prometheus.CounterVec {
	v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help}, labels)
	f.r.MustRegister(v)
	return v
}

func (f factory) gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help}, labels)
	f.r.MustRegister(v)
	return v
}

func (f factory) histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help, Buckets: buckets}, labels)
	f.r.MustRegister(v)
	return v
}
