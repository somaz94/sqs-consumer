# sqs-consumer

A small Amazon SQS consumer written in Go. It long-polls one queue, simulates work for each message, and deletes only the messages it processed successfully. It exists to be a real, observable workload for KEDA scale-to-zero demos: every message takes a configurable amount of time, a special body fails on purpose so redelivery and dead-letter queues can be shown, and the process drains cleanly when KEDA scales the deployment down.

The Helm chart that deploys it with a KEDA `ScaledObject` lives at [somaz94/helm-charts `charts/sqs-consumer`](https://github.com/somaz94/helm-charts/tree/main/charts/sqs-consumer).

<br/>

## How it works

1. `ReceiveMessage` with long polling, up to `SQS_MAX_MESSAGES` per call.
2. Each message in the batch is processed by one of `CONCURRENCY` workers: the consumer logs the message id, body size and receive count, then sleeps `PROCESS_DELAY`.
3. A body of exactly `{"fail":true}` returns an error instead. That message is never deleted, so SQS redelivers it after the visibility timeout and, with a redrive policy, moves it to the DLQ once `maxReceiveCount` is reached.
4. Successful messages are removed with one `DeleteMessageBatch` call per batch.

On `SIGTERM` or `SIGINT` the consumer stops receiving, `/readyz` turns to 503, and the batch in flight gets `SHUTDOWN_TIMEOUT` to finish. Messages still unfinished at that point are left on the queue and reappear after their visibility timeout. The process then exits 0. Set the pod's `terminationGracePeriodSeconds` at least 10 seconds above `SHUTDOWN_TIMEOUT`, which leaves room for the final delete call and the health server shutdown.

<br/>

## Configuration

All settings come from environment variables. Invalid values stop the process at startup with a message naming every offending variable.

| Variable | Default | Description |
|---|---|---|
| `SQS_QUEUE_URL` | required | Queue URL, e.g. `https://sqs.us-east-1.amazonaws.com/123456789012/demo-queue`. `http://` is accepted for local emulators. |
| `AWS_REGION` | from the SDK | Region of the queue. When the SDK finds none, it is taken from an `sqs.<region>.amazonaws.com` queue URL. |
| `SQS_WAIT_TIME_SECONDS` | `20` | Long-poll wait, `0`-`20`. `0` falls back to the queue's own `ReceiveMessageWaitTimeSeconds`. |
| `SQS_MAX_MESSAGES` | `10` | Messages per receive call, `1`-`10`. |
| `SQS_VISIBILITY_TIMEOUT` | `0` | Visibility timeout in seconds for received messages. `0` uses the queue default. |
| `PROCESS_DELAY` | `2s` | Simulated work per message, as a Go duration. |
| `CONCURRENCY` | `1` | Workers processing a received batch, `1`-`10`. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `LOG_FORMAT` | `json` | `json` or `text`. |
| `HEALTH_ADDR` | `:8080` | Listen address for the health endpoints. |
| `SHUTDOWN_TIMEOUT` | `30s` | How long the in-flight batch may run after a shutdown signal. |

Keep the visibility timeout longer than the time a batch takes (`PROCESS_DELAY` times the batch size divided by `CONCURRENCY`), or messages are redelivered while they are still being processed.

The SDK also honours `AWS_ENDPOINT_URL_SQS`, which points the consumer at LocalStack or ElasticMQ.

<br/>

## Health endpoints

| Path | Behaviour |
|---|---|
| `/healthz` | Liveness. Always `200` while the process runs. |
| `/readyz` | Readiness. `503` until the first `ReceiveMessage` call succeeds, `200` after that, and `503` again from the moment shutdown starts. |

<br/>

## Credentials and IAM

The consumer uses the AWS SDK default credential chain only, so IRSA, EKS Pod Identity, an instance profile, or a local profile all work without any change. It never reads static keys from its own configuration.

The consumer's identity needs this policy on the queue:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConsumeQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage"
      ],
      "Resource": "arn:aws:sqs:us-east-1:123456789012:demo-queue"
    },
    {
      "Sid": "KedaQueueLength",
      "Effect": "Allow",
      "Action": "sqs:GetQueueAttributes",
      "Resource": "arn:aws:sqs:us-east-1:123456789012:demo-queue"
    }
  ]
}
```

`sqs:DeleteMessage` also authorizes `DeleteMessageBatch`. The consumer itself never calls `GetQueueAttributes`: KEDA's `aws-sqs-queue` scaler does, to read the queue length, so that statement belongs to whichever identity the `TriggerAuthentication` makes KEDA use. If the queue is encrypted with a customer managed KMS key, the consumer also needs `kms:Decrypt` on that key.

<br/>

## Run locally

With a profile from `~/.aws` (the distroless image runs as `nonroot`, whose home is `/home/nonroot`):

```bash
docker run --rm -p 8080:8080 \
  -v "$HOME/.aws:/home/nonroot/.aws:ro" \
  -e AWS_PROFILE=demo \
  -e SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/demo-queue \
  -e LOG_FORMAT=text \
  ghcr.io/somaz94/sqs-consumer:1.0.0
```

Then send some work, including one message that fails on purpose:

```bash
QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/demo-queue
for i in $(seq 1 20); do
  aws sqs send-message --queue-url "$QUEUE_URL" --message-body "{\"order\":$i}"
done
aws sqs send-message --queue-url "$QUEUE_URL" --message-body '{"fail":true}'
```

`sqs-consumer -version` prints the version, commit and build date.

<br/>

## Development

```bash
make build         # bin/sqs-consumer with version info
make test          # unit tests with the race detector and coverage
make cover         # per-function coverage
make lint          # golangci-lint
make run           # run from source against SQS_QUEUE_URL
make docker-build  # image for the local platform
```

Unit tests run against a small interface over the SQS client, so they need no AWS account or network.

<br/>

## Release

Pushing a `vX.Y.Z` tag runs `.github/workflows/release.yml`, which builds the `linux/amd64` and `linux/arm64` image, pushes it to `ghcr.io/somaz94/sqs-consumer` tagged with the full version, the `major.minor` version, the tag itself and `latest`, and then creates the GitHub release from the git-cliff changelog.

<br/>

## License

Apache License 2.0. See [LICENSE](LICENSE).
