package kraken

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "tidemark/gen/go/tidemark/v1"
)

const DefaultRESTURL = "https://api.kraken.com"

// Backfiller pages GET /0/public/Trades. The `since` cursor is a nanosecond
// timestamp, not a trade id, so a fetch starts slightly before the last trade
// seen ahead of the gap and walks forward until it passes the range.
type Backfiller struct {
	BaseURL string
	Client  *http.Client
	limiter *time.Ticker // docs: ~1 req/s per IP stays within limits
}

func NewBackfiller(baseURL string) *Backfiller {
	if baseURL == "" {
		baseURL = DefaultRESTURL
	}
	return &Backfiller{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 10 * time.Second},
		limiter: time.NewTicker(1100 * time.Millisecond),
	}
}

const maxPages = 100

func (b *Backfiller) Fetch(ctx context.Context, product string, first, last uint64, after time.Time) ([]*pb.Trade, uint64, error) {
	since := "0"
	if !after.IsZero() {
		since = strconv.FormatInt(after.Add(-time.Second).UnixNano(), 10)
	}
	var out []*pb.Trade
	var sourceMax uint64
	for range maxPages {
		rows, next, err := b.page(ctx, Symbol(product), since)
		if err != nil {
			return out, sourceMax, err
		}
		for _, r := range rows {
			sourceMax = max(sourceMax, r.TradeId)
			if r.TradeId >= first && r.TradeId <= last {
				r.ProductId = product
				out = append(out, r)
			}
		}
		if len(rows) == 0 || sourceMax >= last || next == "" || next == since {
			break
		}
		since = next
	}
	return out, sourceMax, nil
}

type tradesResponse struct {
	Error  []string                   `json:"error"`
	Result map[string]json.RawMessage `json:"result"`
}

func (b *Backfiller) page(ctx context.Context, pair, since string) ([]*pb.Trade, string, error) {
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-b.limiter.C:
	}
	u := fmt.Sprintf("%s/0/public/Trades?pair=%s&since=%s&count=1000", b.BaseURL, url.QueryEscape(pair), since)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("kraken rest: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("kraken rest: %s", resp.Status)
	}
	return parseTrades(body)
}

// parseTrades decodes a Trades response. Rows are
// [price, volume, time, side, ordtype, misc, trade_id].
func parseTrades(body []byte) ([]*pb.Trade, string, error) {
	var tr tradesResponse
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := d.Decode(&tr); err != nil {
		return nil, "", fmt.Errorf("kraken rest: decode: %w", err)
	}
	if len(tr.Error) > 0 {
		return nil, "", fmt.Errorf("kraken rest: %s", strings.Join(tr.Error, "; "))
	}
	var next string
	var out []*pb.Trade
	for k, raw := range tr.Result {
		if k == "last" {
			if err := json.Unmarshal(raw, &next); err != nil {
				return nil, "", fmt.Errorf("kraken rest: last: %w", err)
			}
			continue
		}
		var rows [][]json.RawMessage
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, "", fmt.Errorf("kraken rest: rows: %w", err)
		}
		for _, r := range rows {
			if len(r) < 7 {
				return nil, "", fmt.Errorf("kraken rest: short row %d", len(r))
			}
			var price, qty, sd string
			var tid uint64
			if err := firstErr(
				json.Unmarshal(r[0], &price), json.Unmarshal(r[1], &qty),
				json.Unmarshal(r[3], &sd), json.Unmarshal(r[6], &tid),
			); err != nil {
				return nil, "", fmt.Errorf("kraken rest: row: %w", err)
			}
			ts, err := secondsToNanos(string(r[2]))
			if err != nil {
				return nil, "", err
			}
			out = append(out, &pb.Trade{
				Venue:     pb.Venue_VENUE_KRAKEN,
				TradeId:   tid,
				Price:     price,
				Size:      qty,
				TakerSide: side(sd),
				EventTsNs: ts,
				Source:    pb.Source_SOURCE_REST,
			})
		}
	}
	return out, next, nil
}

// secondsToNanos parses "1792654390.913895" without float rounding.
func secondsToNanos(s string) (int64, error) {
	sec, frac, _ := strings.Cut(s, ".")
	if len(frac) > 9 {
		frac = frac[:9]
	}
	frac += strings.Repeat("0", 9-len(frac))
	a, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kraken rest: time %q: %w", s, err)
	}
	b, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kraken rest: time %q: %w", s, err)
	}
	return a*1e9 + b, nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
