# CLAUDE.md — sqs-consumer

A minimal Amazon SQS consumer in Go (aws-sdk-go-v2, stdlib `slog`). It is the demo workload for the KEDA scale-to-zero chart `charts/sqs-consumer` in `somaz94/helm-charts`; the chart lives there, not here.

<br/>

## Build & Test

```bash
make build         # Build bin/sqs-consumer with version ldflags
make test          # go test -race with coverage
make cover         # Per-function coverage
make lint          # golangci-lint (installed into bin/ at the pinned version)
make docker-build  # Image for the local platform
make docker-buildx # Build and push the multi-arch image by hand
```

Keep coverage at 90% or above, `main()` excluded.

<br/>

## Layout

- `cmd/sqs-consumer` holds `main()` and `run()`; `run()` takes its environment, output and SQS client factory as arguments so it is testable
- `internal/config` reads and validates the environment, reporting every bad value at once
- `internal/consumer` is the receive, process, delete loop behind the `API` interface, which tests fake
- `internal/health` serves `/healthz` and `/readyz`

<br/>

## Invariants

- A message is deleted only after its handler returned nil. A failed or abandoned message stays on the queue for redelivery and the DLQ
- The body `{"fail":true}` must fail, exactly as written; the redelivery and DLQ demo depends on it
- Shutdown stops receiving at once and gives the batch in flight `SHUTDOWN_TIMEOUT`. The final delete runs detached from that deadline, so finished work is not redelivered
- `/readyz` stays 503 for good once shutdown starts
- Credentials come only from the SDK default chain (IRSA, EKS Pod Identity). No static keys in code, docs or examples
- Version info is injected with `-X main.version`, `main.commit` and `main.date`; the Dockerfile and Makefile must keep those names

<br/>

## Release

Push a `vX.Y.Z` tag. `release.yml` builds the multi-arch image, pushes it to `ghcr.io/somaz94/sqs-consumer` with `GITHUB_TOKEN`, then creates the GitHub release from git-cliff (`RELEASE.md` is regenerated on every tag; an empty checked-in file is expected).
