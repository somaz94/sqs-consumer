# Build on the runner's native platform and cross-compile, so multi-arch builds need no QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /workspace

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags="-s -w \
      -X main.version=${VERSION} \
      -X main.commit=${GIT_COMMIT} \
      -X main.date=${BUILD_DATE}" \
    -o sqs-consumer ./cmd/sqs-consumer

FROM gcr.io/distroless/static:nonroot
# ARGs do not carry across stages.
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="sqs-consumer" \
      org.opencontainers.image.description="Minimal Amazon SQS consumer used as a KEDA scale-to-zero demo workload" \
      org.opencontainers.image.url="https://github.com/somaz94/sqs-consumer" \
      org.opencontainers.image.source="https://github.com/somaz94/sqs-consumer" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

WORKDIR /
COPY --from=builder /workspace/sqs-consumer .
USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/sqs-consumer"]
