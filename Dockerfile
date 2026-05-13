# syntax=docker/dockerfile:1.7
#
# Single Dockerfile for both binaries. docker-compose picks which `command`
# to run in each service. One image keeps the registry footprint small and
# the build cache shared between the two binaries (they share 90% of their
# dependency graph).

FROM golang:1.26-alpine AS build
WORKDIR /src

# Layer caching: copy go.mod/sum first so dependency download is only redone
# when those files change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/poller ./cmd/poller \
 && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/wsserver ./cmd/wsserver

FROM alpine:3.20
RUN adduser -D -u 10001 app && apk add --no-cache ca-certificates
USER app
COPY --from=build /out/poller /usr/local/bin/poller
COPY --from=build /out/wsserver /usr/local/bin/wsserver

# Default to wsserver since that's the long-running, port-exposing one.
# docker-compose overrides this for the poller service.
CMD ["wsserver"]
