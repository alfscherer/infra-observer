# One image, many roles: the container command selects collector, processor,
# api, automation-worker, simulator or webhook-sink. The JavaScript runtime is
# compiled into the binary; there is no Node.js anywhere in this image.

# ---- build --------------------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# Dependencies first so they are cached across source-only changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/infra-observer ./cmd/infra-observer

# ---- runtime ------------------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget \
 && addgroup -S observer && adduser -S -G observer -H -h /app observer
WORKDIR /app
COPY --from=build /out/infra-observer /usr/local/bin/infra-observer
# Configuration, SNMP profiles and JavaScript extensions ship with the image and
# can be replaced by mounting over them (see OPERATIONS.md).
COPY configs/ /app/configs/
COPY scripts/ /app/scripts/
USER observer
EXPOSE 8080 9090
ENTRYPOINT ["infra-observer"]
CMD ["help"]
