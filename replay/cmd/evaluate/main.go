// Command evaluate scores detectors against injected ground truth.
//
// It reads every InjectionLabel from labels.v1<suffix> and every Alert from
// alerts.v1<suffix>, matches them, and sweeps each detector's score threshold:
//
//	an alert is a true positive if it matches a label: same detector/pattern,
//	same product (and venue, except cross-venue dislocation), and its evidence
//	window overlaps [label start - pre, label end + post];
//	a label is detected if at least one alert at or above the threshold matches it.
//
// Alerts that match no label are "unlabelled". On real market data some of
// them are genuine events (a real volume burst is a volume spike), so
// precision against labels is a lower bound. The operating point is chosen
// the way surveillance desks set thresholds: the best F1 among thresholds
// that keep unlabelled alerts within an hourly budget.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pb "tidemark/gen/go/tidemark/v1"
)

type label struct {
	*pb.InjectionLabel
	strength float64
}

func main() {
	var (
		brokers = flag.String("brokers", "localhost:19092", "Kafka brokers")
		suffix  = flag.String("topic-suffix", ".inj", "topic suffix of the evaluation run")
		pre     = flag.Duration("pre", 5*time.Second, "match tolerance before a label starts")
		post    = flag.Duration("post", 30*time.Second, "match tolerance after a label ends")
		budget  = flag.Float64("budget-per-hour", 6, "max unlabelled alerts per hour per detector at the chosen threshold")
		out     = flag.String("out", "", "write the markdown report here (default stdout)")
		fixed   = flag.String("fixed", "", "score at these thresholds instead of choosing (held-out test), e.g. volume_spike=12,dislocation=4.6")
	)
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var labels []label
	var alerts []*pb.Alert
	must(readAll(ctx, *brokers, "labels.v1"+*suffix, func(b []byte) {
		var l pb.InjectionLabel
		if proto.Unmarshal(b, &l) == nil {
			labels = append(labels, label{&l, strength(&l)})
		}
	}))
	must(readAll(ctx, *brokers, "alerts.v1"+*suffix, func(b []byte) {
		var a pb.Alert
		if proto.Unmarshal(b, &a) == nil {
			alerts = append(alerts, &a)
		}
	}))
	alerts = dedup(alerts)
	hours := spanHours(labels, alerts)
	fixedTh := map[string]float64{}
	for _, kv := range strings.Split(*fixed, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
			f, err := strconv.ParseFloat(v, 64)
			must(err)
			fixedTh[k] = f
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Generated %s by `evaluate --topic-suffix %s` (match window: label −%s / +%s).\n\n",
		time.Now().UTC().Format("2006-01-02 15:04 UTC"), *suffix, *pre, *post)
	fmt.Fprintf(&sb, "Labels: %d injected patterns. Alerts: %d distinct firings. Stream time covered: %.2f h.\n\n", len(labels), len(alerts), hours)

	detectors := []string{"volume_spike", "spoof_layering", "dislocation", "wash_trade"}
	fmt.Fprintln(&sb, "## Operating points")
	fmt.Fprintln(&sb)
	if *fixed != "" {
		fmt.Fprintf(&sb, "Thresholds fixed in advance (held-out evaluation): `%s`.\n\n", *fixed)
	} else {
		fmt.Fprintf(&sb, "Chosen threshold = best F1 among thresholds with at most %.0f unlabelled alerts per hour (else the strictest threshold).\n\n", *budget)
	}
	fmt.Fprintln(&sb, "| Detector | Injected | Threshold | TP alerts | Unlabelled alerts | Unlabelled / h | Precision (vs labels) | Labels detected | Recall | F1 |")
	fmt.Fprintln(&sb, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	var sections strings.Builder
	for _, d := range detectors {
		ls := filterLabels(labels, d)
		as := filterAlerts(alerts, d)
		matches := match(ls, as, *pre, *post)
		points := sweep(ls, as, matches)
		best := choose(points, *budget*hours)
		if th, ok := fixedTh[d]; ok {
			p := at(ls, as, matches, th)
			best = &p
			points = append(points, p)
			sort.Slice(points, func(i, j int) bool { return points[i].threshold < points[j].threshold })
		}
		if best == nil {
			fmt.Fprintf(&sb, "| %s | %d | — | 0 | 0 | 0 | — | 0 | 0.00 | — |\n", d, len(ls))
			continue
		}
		fmt.Fprintf(&sb, "| %s | %d | %.3g | %d | %d | %.1f | %.3f | %d / %d | %.3f | %.3f |\n",
			d, len(ls), best.threshold, best.tp, best.fp, float64(best.fp)/hours, best.precision, best.detected, len(ls), best.recall, best.f1)

		fmt.Fprintf(&sections, "\n### %s\n\n", d)
		fmt.Fprintln(&sections, "| Threshold | TP | Unlabelled | Unlabelled / h | Precision | Recall | F1 |")
		fmt.Fprintln(&sections, "|---:|---:|---:|---:|---:|---:|---:|")
		shown := thin(points, 12)
		if !contains(shown, best.threshold) {
			shown = append(shown, *best)
			sort.Slice(shown, func(i, j int) bool { return shown[i].threshold < shown[j].threshold })
		}
		for _, p := range shown {
			mark := ""
			if p.threshold == best.threshold {
				mark = " ◀"
			}
			fmt.Fprintf(&sections, "| %.3g%s | %d | %d | %.1f | %.3f | %.3f | %.3f |\n", p.threshold, mark, p.tp, p.fp, float64(p.fp)/hours, p.precision, p.recall, p.f1)
		}
		fmt.Fprintln(&sections)
		fmt.Fprintln(&sections, "Recall by injection strength at the chosen threshold:")
		fmt.Fprintln(&sections)
		fmt.Fprintln(&sections, "| Strength | Injected | Detected |")
		fmt.Fprintln(&sections, "|---|---:|---:|")
		for _, row := range byStrength(d, ls, as, matches, best.threshold) {
			fmt.Fprintln(&sections, row)
		}
		var missed []string
		for i, l := range ls {
			if !detectedAt(i, as, matches, best.threshold) {
				missed = append(missed, fmt.Sprintf("%s (%s, strength %.3g)", l.InjectionId, l.ProductId, l.strength))
			}
		}
		if len(missed) > 0 {
			fmt.Fprintf(&sections, "\nMissed: %s\n", strings.Join(missed, "; "))
		}
		var fps []string
		for j, a := range as {
			if a.Score >= best.threshold && len(matches.byAlert[j]) == 0 {
				fps = append(fps, fmt.Sprintf("`%s` (score %.3g): %s", a.AlertId, a.Score, a.Summary))
			}
		}
		if len(fps) > 0 {
			if len(fps) > 15 {
				fps = append(fps[:15], fmt.Sprintf("… and %d more", len(fps)-15))
			}
			fmt.Fprintf(&sections, "\nUnlabelled alerts at the chosen threshold (real market events or false positives; candidates for hand-labelling):\n\n- %s\n",
				strings.Join(fps, "\n- "))
		}
	}
	sb.WriteString(sections.String())

	if *out == "" {
		fmt.Print(sb.String())
	} else {
		must(os.WriteFile(*out, []byte(sb.String()), 0o644))
		fmt.Println("wrote", *out)
	}
}

// strength is the injected magnitude used for recall-by-strength buckets.
func strength(l *pb.InjectionLabel) float64 {
	get := func(k string) float64 { v, _ := strconv.ParseFloat(l.Params[k], 64); return v }
	switch l.Pattern {
	case "volume_spike":
		return get("multiple")
	case "spoof_layering":
		return get("depth_share")
	case "dislocation":
		v := get("bps")
		if v < 0 {
			v = -v
		}
		return v
	case "wash_trade":
		if l.Params["kind"] == "self_trade" {
			return 1
		}
		return 1 - get("size_jitter")
	}
	return 0
}

func filterLabels(ls []label, d string) []label {
	var out []label
	for _, l := range ls {
		if l.Pattern == d {
			out = append(out, l)
		}
	}
	return out
}

func filterAlerts(as []*pb.Alert, d string) []*pb.Alert {
	var out []*pb.Alert
	for _, a := range as {
		if a.Detector == d {
			out = append(out, a)
		}
	}
	return out
}

// dedup keeps one alert per id: the highest-scoring version. Detectors
// re-emit an escalating episode under the same id, and at-least-once
// delivery can repeat a message.
func dedup(as []*pb.Alert) []*pb.Alert {
	best := map[string]*pb.Alert{}
	var order []string
	for _, a := range as {
		cur, ok := best[a.AlertId]
		if !ok {
			order = append(order, a.AlertId)
		}
		if !ok || a.Score > cur.Score {
			best[a.AlertId] = a
		}
	}
	out := make([]*pb.Alert, 0, len(order))
	for _, id := range order {
		out = append(out, best[id])
	}
	return out
}

type matchSet struct {
	byLabel [][]int // label index -> alert indexes
	byAlert [][]int // alert index -> label indexes
}

func match(ls []label, as []*pb.Alert, pre, post time.Duration) matchSet {
	m := matchSet{byLabel: make([][]int, len(ls)), byAlert: make([][]int, len(as))}
	for i, l := range ls {
		lo, hi := l.StartTsNs-int64(pre), l.EndTsNs+int64(post)
		for j, a := range as {
			if a.ProductId != l.ProductId {
				continue
			}
			if l.Pattern != "dislocation" && a.Venue != l.Venue {
				continue
			}
			if a.WindowEndNs >= lo && a.WindowStartNs <= hi {
				m.byLabel[i] = append(m.byLabel[i], j)
				m.byAlert[j] = append(m.byAlert[j], i)
			}
		}
	}
	return m
}

type point struct {
	threshold             float64
	tp, fp, detected      int
	precision, recall, f1 float64
}

// at scores the detector at one threshold.
func at(ls []label, as []*pb.Alert, m matchSet, th float64) point {
	p := point{threshold: th}
	for j, a := range as {
		if a.Score < th {
			continue
		}
		if len(m.byAlert[j]) > 0 {
			p.tp++
		} else {
			p.fp++
		}
	}
	for i := range ls {
		if detectedAt(i, as, m, th) {
			p.detected++
		}
	}
	if p.tp+p.fp > 0 {
		p.precision = float64(p.tp) / float64(p.tp+p.fp)
	}
	if len(ls) > 0 {
		p.recall = float64(p.detected) / float64(len(ls))
	}
	if p.precision+p.recall > 0 {
		p.f1 = 2 * p.precision * p.recall / (p.precision + p.recall)
	}
	return p
}

func sweep(ls []label, as []*pb.Alert, m matchSet) []point {
	ths := map[float64]bool{}
	for _, a := range as {
		ths[a.Score] = true
	}
	var sorted []float64
	for t := range ths {
		sorted = append(sorted, t)
	}
	sort.Float64s(sorted)
	var out []point
	for _, th := range sorted {
		p := point{threshold: th}
		for j, a := range as {
			if a.Score < th {
				continue
			}
			if len(m.byAlert[j]) > 0 {
				p.tp++
			} else {
				p.fp++
			}
		}
		for i := range ls {
			if detectedAt(i, as, m, th) {
				p.detected++
			}
		}
		if p.tp+p.fp > 0 {
			p.precision = float64(p.tp) / float64(p.tp+p.fp)
		}
		if len(ls) > 0 {
			p.recall = float64(p.detected) / float64(len(ls))
		}
		if p.precision+p.recall > 0 {
			p.f1 = 2 * p.precision * p.recall / (p.precision + p.recall)
		}
		out = append(out, p)
	}
	return out
}

func detectedAt(i int, as []*pb.Alert, m matchSet, th float64) bool {
	for _, j := range m.byLabel[i] {
		if as[j].Score >= th {
			return true
		}
	}
	return false
}

// choose returns the best-F1 point whose unlabelled alerts fit the budget
// (ties go to the higher recall), else the strictest threshold.
func choose(ps []point, maxUnlabelled float64) *point {
	var best *point
	for i := range ps {
		p := &ps[i]
		if float64(p.fp) > maxUnlabelled {
			continue
		}
		if best == nil || p.f1 > best.f1 || (p.f1 == best.f1 && p.recall > best.recall) {
			best = p
		}
	}
	if best == nil && len(ps) > 0 {
		best = &ps[len(ps)-1]
	}
	return best
}

func contains(ps []point, th float64) bool {
	for _, p := range ps {
		if p.threshold == th {
			return true
		}
	}
	return false
}

// spanHours is the stream time covered, from the earliest to the latest label or alert.
func spanHours(ls []label, as []*pb.Alert) float64 {
	var lo, hi int64
	upd := func(a, b int64) {
		if a > 0 && (lo == 0 || a < lo) {
			lo = a
		}
		if b > hi {
			hi = b
		}
	}
	for _, l := range ls {
		upd(l.StartTsNs, l.EndTsNs)
	}
	for _, a := range as {
		upd(a.WindowStartNs, a.WindowEndNs)
	}
	if hi <= lo {
		return 1
	}
	return float64(hi-lo) / float64(time.Hour)
}

func thin(ps []point, n int) []point {
	if len(ps) <= n {
		return ps
	}
	var out []point
	for i := 0; i < n; i++ {
		out = append(out, ps[i*(len(ps)-1)/(n-1)])
	}
	return out
}

func byStrength(d string, ls []label, as []*pb.Alert, m matchSet, th float64) []string {
	var edges []float64
	unit := "×"
	switch d {
	case "volume_spike":
		edges = []float64{0, 5, 10, 20, 1e9}
	case "spoof_layering":
		edges = []float64{0, 0.5, 1, 2, 1e9}
	case "dislocation":
		edges, unit = []float64{0, 10, 20, 40, 1e9}, " bps"
	case "wash_trade":
		edges, unit = []float64{0, 0.98, 0.999, 1.01}, ""
	}
	var rows []string
	for k := 0; k+1 < len(edges); k++ {
		var n, hit int
		for i, l := range ls {
			if l.strength >= edges[k] && l.strength < edges[k+1] {
				n++
				if detectedAt(i, as, m, th) {
					hit++
				}
			}
		}
		hiTxt := fmt.Sprintf("%g%s", edges[k+1], unit)
		if edges[k+1] >= 1e8 {
			hiTxt = "∞"
		}
		rows = append(rows, fmt.Sprintf("| [%g%s, %s) | %d | %d |", edges[k], unit, hiTxt, n, hit))
	}
	return rows
}

func readAll(ctx context.Context, brokers, topic string, fn func([]byte)) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	target := map[int32]int64{}
	assign := map[string]map[int32]kgo.Offset{topic: {}}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			target[o.Partition] = o.Offset
			assign[topic][o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	if len(target) == 0 {
		return nil
	}
	cl.AddConsumePartitions(assign)
	var h sr.ConfluentHeader
	for len(target) > 0 {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= target[r.Partition] {
				delete(target, r.Partition)
			}
			_, rest, err := h.DecodeID(r.Value)
			if err != nil {
				return
			}
			_, body, err := h.DecodeIndex(rest, 8)
			if err == nil {
				fn(body)
			}
		})
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "evaluate:", err)
		os.Exit(1)
	}
}
