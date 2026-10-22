package kraken

import (
	"fmt"
	"hash/crc32"
	"math/big"
	"slices"
	"strconv"
	"strings"
)

// checksumLevels is fixed by Kraken regardless of subscribed depth.
const checksumLevels = 10

// Book is a local order book for one symbol. Prices and quantities are held as
// integers scaled by 10^precision, which keeps them exact and makes the
// checksum string a plain decimal rendering of each integer.
type Book struct {
	PricePrec int
	QtyPrec   int
	Depth     int
	bids      map[int64]int64
	asks      map[int64]int64
}

func NewBook(pricePrec, qtyPrec, depth int) *Book {
	return &Book{PricePrec: pricePrec, QtyPrec: qtyPrec, Depth: depth,
		bids: map[int64]int64{}, asks: map[int64]int64{}}
}

// Level is one price level as sent by Kraken (exact JSON number text).
type Level struct {
	Price string
	Qty   string
}

// Apply sets or removes levels on one side. A zero quantity removes the level.
func (b *Book) Apply(levels []Level, bid bool) error {
	side := b.asks
	if bid {
		side = b.bids
	}
	for _, l := range levels {
		p, err := ParseScaled(l.Price, b.PricePrec)
		if err != nil {
			return fmt.Errorf("price %q: %w", l.Price, err)
		}
		q, err := ParseScaled(l.Qty, b.QtyPrec)
		if err != nil {
			return fmt.Errorf("qty %q: %w", l.Qty, err)
		}
		if q == 0 {
			delete(side, p)
		} else {
			side[p] = q
		}
	}
	return nil
}

// Truncate drops levels beyond the subscribed depth, as Kraken requires.
func (b *Book) Truncate() {
	truncate(b.bids, b.Depth, true)
	truncate(b.asks, b.Depth, false)
}

func truncate(side map[int64]int64, depth int, desc bool) {
	if depth <= 0 || len(side) <= depth {
		return
	}
	for _, p := range sortedPrices(side, desc)[depth:] {
		delete(side, p)
	}
}

func sortedPrices(side map[int64]int64, desc bool) []int64 {
	ps := make([]int64, 0, len(side))
	for p := range side {
		ps = append(ps, p)
	}
	slices.Sort(ps)
	if desc {
		slices.Reverse(ps)
	}
	return ps
}

// Checksum is CRC32 over the top 10 asks (ascending) then the top 10 bids
// (descending). Each level contributes price then qty, with the decimal point
// removed and leading zeros stripped, which is exactly the scaled integer.
func (b *Book) Checksum() uint32 {
	var sb strings.Builder
	write := func(side map[int64]int64, desc bool) {
		ps := sortedPrices(side, desc)
		if len(ps) > checksumLevels {
			ps = ps[:checksumLevels]
		}
		for _, p := range ps {
			sb.WriteString(strconv.FormatInt(p, 10))
			sb.WriteString(strconv.FormatInt(side[p], 10))
		}
	}
	write(b.asks, false)
	write(b.bids, true)
	return crc32.ChecksumIEEE([]byte(sb.String()))
}

// Format renders a scaled integer back to decimal text with full precision.
func Format(v int64, prec int) string {
	s := strconv.FormatInt(v, 10)
	if prec == 0 {
		return s
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if len(s) <= prec {
		s = strings.Repeat("0", prec-len(s)+1) + s
	}
	s = s[:len(s)-prec] + "." + s[len(s)-prec:]
	if neg {
		s = "-" + s
	}
	return s
}

// ParseScaled converts decimal text such as "82200.0" or "1e-05" to an integer
// scaled by 10^prec. It fails if the value has more precision than prec.
func ParseScaled(text string, prec int) (int64, error) {
	if !strings.ContainsAny(text, "eE") {
		neg := strings.HasPrefix(text, "-")
		t := strings.TrimPrefix(text, "-")
		intPart, frac, _ := strings.Cut(t, ".")
		if len(frac) > prec {
			if strings.TrimRight(frac[prec:], "0") != "" {
				return 0, fmt.Errorf("more than %d decimals", prec)
			}
			frac = frac[:prec]
		}
		frac += strings.Repeat("0", prec-len(frac))
		v, err := strconv.ParseInt(intPart+frac, 10, 64)
		if err != nil {
			return 0, err
		}
		if neg {
			v = -v
		}
		return v, nil
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return 0, fmt.Errorf("not a number")
	}
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(prec)), nil)))
	if !r.IsInt() {
		return 0, fmt.Errorf("more than %d decimals", prec)
	}
	return r.Num().Int64(), nil
}

// decimals counts digits after the point in plain decimal text; used when the
// instrument precision is not known yet.
func decimals(text string) int {
	_, frac, ok := strings.Cut(text, ".")
	if !ok {
		return 0
	}
	return len(frac)
}
