// Command reconcile proves (and repairs) lakehouse completeness. For a time
// window it compares the trade ids in iceberg.market.raw_trades with what the
// venue says exists:
//
//  1. find every run of missing trade ids (gaps-and-islands in Trino);
//  2. re-fetch each run from the venue's REST API;
//  3. produce the trades it finds to trades.v1 (source=rest, ingest_id=reconcile),
//     where Flink dedups and commits them to Iceberg like any other trade;
//  4. record the run in iceberg.ops.reconciliation_runs.
//
// Ids the REST API confirms never existed are reported as absent at source.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"

	pb "tidemark/gen/go/tidemark/v1"
	"tidemark/ingest/metrics"
	"tidemark/ingest/sink"
	"tidemark/ingest/venue"
	"tidemark/ingest/venue/coinbase"
	"tidemark/ingest/venue/kraken"
)

type productReport struct {
	Venue           string  `json:"venue"`
	Product         string  `json:"product"`
	ExpectedIDs     int64   `json:"expected_ids"`
	PresentIDs      int64   `json:"present_ids"`
	DuplicateRows   int64   `json:"duplicate_rows"`
	MissingRanges   int     `json:"missing_ranges"`
	MissingIDs      int64   `json:"missing_ids"`
	Refetched       int64   `json:"refetched"`
	AbsentAtSource  int64   `json:"absent_at_source"`
	StillMissing    int64   `json:"still_missing"`
	CompletenessPct float64 `json:"completeness_pct_before_repair"`
}

type missingRange struct {
	venue, product string
	first, last    uint64
	before         time.Time
}

func main() {
	var (
		trinoDSN = flag.String("trino", "http://tidemark@localhost:8088?catalog=iceberg&schema=market", "Trino DSN")
		brokers  = flag.String("brokers", "localhost:19092", "Kafka brokers")
		registry = flag.String("registry", "http://localhost:18081", "schema registry URL")
		window   = flag.Duration("window", 2*time.Hour, "check trades with event time in [now-window-settle, now-settle]")
		settle   = flag.Duration("settle", 10*time.Minute, "skip the most recent data (still in flight through Flink)")
		repair   = flag.Bool("repair", true, "re-fetch missing ranges and produce them to Kafka")
		maxIDs   = flag.Int64("max-ids", 200_000, "skip ranges larger than this (report only)")
		record   = flag.Bool("record", true, "write the run to iceberg.ops.reconciliation_runs")
		catchUp  = flag.Duration("catch-up", 48*time.Hour, "start at the end of the last recorded run if that is earlier than now-window "+
			"(covers downtime of the scheduler itself), looking back at most this far; 0 disables")
	)
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	db, err := sql.Open("trino", *trinoDSN)
	if err != nil {
		fail(err)
	}
	defer db.Close()

	runID := time.Now().UTC().Format("20060102T150405Z")
	hi := time.Now().UTC().Add(-*settle)
	lo := hi.Add(-*window)
	if *catchUp > 0 {
		// A fixed look-back misses holes older than the window when the scheduler itself was down:
		// after a 3.5 h outage the hourly 2 h window never saw the first 50 minutes of it.
		if last, ok := lastRunEnd(ctx, db); ok && last.Before(lo) {
			lo = last
			if floor := hi.Add(-*catchUp); lo.Before(floor) {
				lo = floor
			}
			slog.Info("catching up from the last recorded run", "from", lo.Format(time.RFC3339))
		}
	}
	slog.Info("reconcile", "run", runID, "from", lo.Format(time.RFC3339), "to", hi.Format(time.RFC3339), "repair", *repair)

	reports, ranges, err := scan(ctx, db, lo, hi)
	if err != nil {
		fail(err)
	}

	if *repair && len(ranges) > 0 {
		m := metrics.New()
		k, err := sink.NewKafka(ctx, sink.KafkaConfig{
			Brokers: strings.Split(*brokers, ","), RegistryURL: *registry,
			Topics: sink.DefaultTopics(""), ClientID: "tidemark-reconcile",
		}, m)
		if err != nil {
			fail(err)
		}
		backfill := map[string]venue.Backfiller{
			"coinbase": coinbase.NewBackfiller(""),
			"kraken":   kraken.NewBackfiller(""),
		}
		for _, r := range ranges {
			rep := reports[r.venue+":"+r.product]
			span := int64(r.last - r.first + 1)
			if span > *maxIDs {
				slog.Warn("range too large; report only", "venue", r.venue, "product", r.product, "span", span)
				rep.StillMissing += span
				continue
			}
			trades, sourceMax, err := backfill[r.venue].Fetch(ctx, r.product, r.first, r.last, r.before)
			if err != nil {
				slog.Error("refetch", "venue", r.venue, "product", r.product, "first", r.first, "last", r.last, "err", err)
				rep.StillMissing += span
				continue
			}
			now := time.Now().UnixNano()
			for _, t := range trades {
				t.Source = pb.Source_SOURCE_REST
				t.IngestId = "reconcile"
				t.RecvTsNs = now
				k.Trade(t)
			}
			got := int64(len(trades))
			rep.Refetched += got
			if sourceMax >= r.last {
				rep.AbsentAtSource += span - got
			} else {
				rep.StillMissing += span - got
			}
			slog.Info("range repaired", "venue", r.venue, "product", r.product, "first", r.first, "last", r.last,
				"span", span, "refetched", got, "source_max", sourceMax)
		}
		if err := k.Flush(ctx); err != nil {
			fail(err)
		}
		k.Close()
	} else {
		for _, r := range ranges {
			reports[r.venue+":"+r.product].StillMissing += int64(r.last - r.first + 1)
		}
	}

	var out []*productReport
	var exp, present, dup, refetched, absent, still int64
	for _, r := range reports {
		out = append(out, r)
		exp += r.ExpectedIDs
		present += r.PresentIDs
		dup += r.DuplicateRows
		refetched += r.Refetched
		absent += r.AbsentAtSource
		still += r.StillMissing
	}
	pct := 100.0
	if exp > 0 {
		pct = 100 * float64(present) / float64(exp)
	}
	summary := map[string]any{
		"run_id": runID, "window_start": lo, "window_end": hi,
		"expected_ids": exp, "present_ids": present, "completeness_pct_before_repair": pct,
		"duplicate_rows": dup, "refetched": refetched, "absent_at_source": absent, "still_missing": still,
		"products": out,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(summary)

	if *record {
		if err := recordRun(ctx, db, runID, lo, hi, out); err != nil {
			slog.Error("record run", "err", err)
			os.Exit(1)
		}
	}
}

// scan computes per-product coverage and the missing id runs inside the window.
func scan(ctx context.Context, db *sql.DB, lo, hi time.Time) (map[string]*productReport, []missingRange, error) {
	reports := map[string]*productReport{}
	const coverage = `
SELECT venue, product_id,
       max(trade_id) - min(trade_id) + 1 AS expected,
       count(DISTINCT trade_id)          AS present,
       count(*) - count(DISTINCT trade_id) AS dup
FROM iceberg.market.raw_trades
WHERE event_ts >= from_iso8601_timestamp(?) AND event_ts < from_iso8601_timestamp(?)
GROUP BY 1, 2`
	rows, err := db.QueryContext(ctx, coverage, lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano))
	if err != nil {
		return nil, nil, fmt.Errorf("coverage query: %w", err)
	}
	for rows.Next() {
		r := &productReport{}
		if err := rows.Scan(&r.Venue, &r.Product, &r.ExpectedIDs, &r.PresentIDs, &r.DuplicateRows); err != nil {
			return nil, nil, err
		}
		if r.ExpectedIDs > 0 {
			r.CompletenessPct = 100 * float64(r.PresentIDs) / float64(r.ExpectedIDs)
		}
		reports[r.Venue+":"+r.Product] = r
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	const gaps = `
WITH ids AS (
  SELECT DISTINCT venue, product_id, trade_id, event_ts
  FROM iceberg.market.raw_trades
  WHERE event_ts >= from_iso8601_timestamp(?) AND event_ts < from_iso8601_timestamp(?)
), ordered AS (
  SELECT venue, product_id, trade_id, event_ts,
         lead(trade_id) OVER (PARTITION BY venue, product_id ORDER BY trade_id) AS next_id
  FROM ids
)
SELECT venue, product_id, trade_id + 1, next_id - 1, to_unixtime(event_ts)
FROM ordered
WHERE next_id > trade_id + 1
ORDER BY 1, 2, 3`
	rows, err = db.QueryContext(ctx, gaps, lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano))
	if err != nil {
		return nil, nil, fmt.Errorf("gaps query: %w", err)
	}
	defer rows.Close()
	var ranges []missingRange
	for rows.Next() {
		var r missingRange
		var first, last int64
		var before float64
		if err := rows.Scan(&r.venue, &r.product, &first, &last, &before); err != nil {
			return nil, nil, err
		}
		r.first, r.last = uint64(first), uint64(last)
		r.before = time.Unix(0, int64(before*1e9))
		rep := reports[r.venue+":"+r.product]
		rep.MissingRanges++
		rep.MissingIDs += last - first + 1
		ranges = append(ranges, r)
	}
	return reports, ranges, rows.Err()
}

// lastRunEnd is the end of the newest recorded run window, if any.
func lastRunEnd(ctx context.Context, db *sql.DB) (time.Time, bool) {
	var end sql.NullTime
	if err := db.QueryRowContext(ctx, "SELECT max(window_end) FROM iceberg.ops.reconciliation_runs").Scan(&end); err != nil || !end.Valid {
		return time.Time{}, false
	}
	return end.Time.UTC(), true
}

func recordRun(ctx context.Context, db *sql.DB, runID string, lo, hi time.Time, reps []*productReport) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS iceberg.ops`,
		`CREATE TABLE IF NOT EXISTS iceberg.ops.reconciliation_runs (
            run_id varchar, run_ts timestamp(6) with time zone,
            window_start timestamp(6) with time zone, window_end timestamp(6) with time zone,
            venue varchar, product_id varchar,
            expected_ids bigint, present_ids bigint, duplicate_rows bigint,
            missing_ranges integer, missing_ids bigint, refetched bigint,
            absent_at_source bigint, still_missing bigint, completeness_pct double)
         WITH (format = 'PARQUET', partitioning = ARRAY['day(run_ts)'])`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	if len(reps) == 0 {
		return nil
	}
	var vals []string
	ts := func(t time.Time) string {
		return fmt.Sprintf("from_iso8601_timestamp('%s')", t.UTC().Format(time.RFC3339Nano))
	}
	for _, r := range reps {
		vals = append(vals, fmt.Sprintf("('%s', %s, %s, %s, '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %f)",
			runID, ts(time.Now()), ts(lo), ts(hi), r.Venue, r.Product, r.ExpectedIDs, r.PresentIDs, r.DuplicateRows,
			r.MissingRanges, r.MissingIDs, r.Refetched, r.AbsentAtSource, r.StillMissing, r.CompletenessPct))
	}
	_, err := db.ExecContext(ctx, "INSERT INTO iceberg.ops.reconciliation_runs VALUES "+strings.Join(vals, ", "))
	return err
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "reconcile:", err)
	os.Exit(1)
}
