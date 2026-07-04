# syntax=docker/dockerfile:1

# Builds any one of the cmd/* binaries via --build-arg BINARY=<name>.
# Example:
#   docker build --build-arg BINARY=coreapi -t jengine-coreapi .
#   docker build --build-arg BINARY=ingestion-gateway -t jengine-ingestion-gateway .
#
# One Dockerfile for all 6 cmd/* entrypoints (coreapi, ingestion-gateway,
# matching-batch, matching-stream, webhook-dispatcher, api-gateway) rather
# than six near-identical files - see plans/task/core/01 for the binary list.

FROM golang:1.22-alpine AS build
ARG BINARY=coreapi
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./cmd/${BINARY}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
