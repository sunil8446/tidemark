package main

import (
	"fmt"
	"net/http"
	"time"

	"tidemark/ingest/pipeline"
	"tidemark/ingest/wsconn"
)

// registerDebug exposes fault injection for one venue:
//
//	POST /debug/drop/<venue>?for=20s   discard frames for a while (forces a gap)
//	POST /debug/reconnect/<venue>      drop the WebSocket session now
func registerDebug(mux *http.ServeMux, name string, p *pipeline.Pipeline, r *wsconn.Runner) {
	mux.HandleFunc("POST /debug/drop/"+name, func(w http.ResponseWriter, req *http.Request) {
		d, err := time.ParseDuration(req.URL.Query().Get("for"))
		if err != nil || d <= 0 || d > 10*time.Minute {
			http.Error(w, "for must be a duration in (0, 10m]", http.StatusBadRequest)
			return
		}
		p.DropFor(d)
		fmt.Fprintf(w, "dropping %s frames for %s\n", name, d)
	})
	mux.HandleFunc("POST /debug/reconnect/"+name, func(w http.ResponseWriter, _ *http.Request) {
		r.Reconnect()
		fmt.Fprintf(w, "reconnecting %s\n", name)
	})
}
