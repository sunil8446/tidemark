// Package schemas exposes the Protobuf sources so they can be registered with
// the schema registry at startup.
package schemas

import _ "embed"

// MarketProto is the text of tidemark/v1/market.proto.
//
//go:embed tidemark/v1/market.proto
var MarketProto string

// Message indexes within market.proto, used in the Confluent wire header.
// Enums do not count; only top-level messages, in declaration order.
var (
	IndexTrade      = []int{0}
	IndexBookUpdate = []int{2}
	IndexGapEvent   = []int{3}
)
