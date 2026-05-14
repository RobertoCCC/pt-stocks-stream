# syntax=docker/dockerfile:1.7
#
# One Dockerfile, three binaries:
#   - poller    : split-deploy publisher (paid Render plan or any worker host)
#   - wsserver  : split-deploy subscriber + WebSocket gateway
#   - allinone  : free-tier shape — poller + wsserver in one process, both
#                 connected through the same Upstash Redis channel
#
# docker-compose picks `poller` and `wsserver` to demonstrate the split
# architecture locally. Render's blueprint targets `allinone` so the whole
# stack fits in a single free web service.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Layer caching: copy go.mod/sum first so dependency download is only redone
# when those files change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/poller   ./cmd/poller   \
 && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/wsserver ./cmd/wsserver \
 && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/allinone ./cmd/allinone

FROM alpine:3.20
RUN adduser -D -u 10001 app && apk add --no-cache ca-certificates
USER app
COPY --from=build /out/poller   /usr/local/bin/poller
COPY --from=build /out/wsserver /usr/local/bin/wsserver
COPY --from=build /out/allinone /usr/local/bin/allinone

# Default to allinone since that's what the deployed image runs. Local
# docker-compose overrides this per-service for the split-deploy demo.
CMD ["allinone"]
