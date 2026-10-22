package main

import (
	"context"
	"log/slog"

	pb "tidemark/gen/go/tidemark/v1"
)

// discard is the dry-run sink: it logs gap events and drops everything else.
type discard struct{}

func (discard) Trade(*pb.Trade)     {}
func (discard) Book(*pb.BookUpdate) {}
func (discard) Gap(g *pb.GapEvent) {
	slog.Info("gap event", "id", g.GapId, "status", g.Status, "first", g.FirstMissingTradeId, "last", g.LastMissingTradeId, "recovered", g.RecoveredCount)
}
func (discard) Flush(context.Context) error { return nil }
func (discard) Close()                      {}
