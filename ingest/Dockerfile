# Go tools image: ingest, replay, audit, reconcile, evaluate. Static, non-root.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY schemas ./schemas
COPY ingest ./ingest
COPY replay ./replay
RUN for c in ingest/cmd/ingest ingest/cmd/audit ingest/cmd/reconcile replay/cmd/replay replay/cmd/evaluate; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$(basename $c) ./$c; done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 9102
ENTRYPOINT ["/usr/local/bin/ingest"]
