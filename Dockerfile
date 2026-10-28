# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
# Pure Go throughout (modernc.org/sqlite, not mattn/go-sqlite3), so the binary
# is static and needs no libc in the final image.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/statuspage ./cmd/statuspage

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 -h /data status
COPY --from=build /out/statuspage /usr/local/bin/statuspage
USER status
WORKDIR /data
VOLUME /data
EXPOSE 8080
# The binary ships its own health probe, so the image needs no curl.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/statuspage", "-healthcheck"]
ENTRYPOINT ["/usr/local/bin/statuspage"]
CMD ["-config", "/etc/statuspage/config.yaml"]
