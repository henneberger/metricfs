# syntax=docker/dockerfile:1.7

FROM golang:1.23-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/metricfs ./cmd/metricfs

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fuse3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/metricfs /usr/local/bin/metricfs
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Needed by fuse_allow_other in many environments.
RUN mkdir -p /etc && printf "user_allow_other\n" > /etc/fuse.conf

ENTRYPOINT ["/entrypoint.sh"]
CMD ["metricfs", "--help"]
