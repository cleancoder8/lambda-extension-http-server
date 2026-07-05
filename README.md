# lambda-extension-http-server

Fire-and-forget S3 uploads from AWS Lambda — using a sidecar [Lambda Extension](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html) that runs an internal HTTP server.

Your handler POSTs to `localhost:4000/upload` and returns immediately. The extension handles the S3 upload asynchronously, with retries, disk-backed queue recovery, and graceful shutdown. Shrink your billed duration to nothing.

## How it works

```
┌──────────────────────── Lambda Execution Environment ──────────────────────┐
│                                                                              │
│  Lambda handler                  Extension (HTTP sidecar)                    │
│  ┌──────────────────┐    POST    ┌─────────────────────────┐                │
│  │                  │  /upload   │  /upload endpoint        │                │
│  │  Wraps event +   │───────────▶│  ├─ Write body → /tmp    │  PutObject     │
│  │  context in JSON  │◀── 202 ───│  ├─ Return 202           │──────▶  S3     │
│  │  Returns to caller│           │  ├─ Pump queue (async)   │                │
│  │  ~5ms billed      │           │  └─ 5 concurrency        │                │
│  └──────────────────┘           │                                   │        │
│                                  │  Disk-backed queue recovery      │        │
│                                  │  Exponential backoff retry       │        │
│                                  │  Graceful shutdown drain         │        │
│                                  └──────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘
```

1. **Lambda handler** receives an event, wraps it with `requestId`, `functionName`, `functionVersion`, and a timestamp.
2. POSTs the JSON payload to the extension's `/upload` endpoint on `localhost:4000`.
3. The extension writes the body to `/tmp/s3-queue/`, responds HTTP 202, and returns — the handler's job is done.
4. An **async upload queue** pumps jobs to S3 (`PutObject`) with 5 concurrent workers and exponential backoff retry.
5. On **cold start**, the extension scans `/tmp/s3-queue/` and recovers any incomplete jobs. Orphaned files are cleaned up.
6. On **Lambda shutdown**, the extension drains active uploads for up to 2 seconds before exiting.

## Why an extension instead of doing it inline?

If your handler does the S3 upload itself, you pay for every millisecond of network I/O. With this extension, the handler fires off a local HTTP call (~1-5ms) and returns. The extension pays the S3 latency tax *after* your customer got a response. This matters if you're doing high-throughput event processing where duration = cost.

Additionally, the extension provides durability you'd otherwise have to build yourself: disk persistence, recovery, retry, and shutdown safety.

## Project structure

```
src/
├── extension/index.ts    # Lambda Extension — HTTP server, queue, S3 client
└── lambda/index.ts       # Lambda handler — wraps event and POSTs to extension
lib/
└── lambda-extension-stack.ts  # CDK stack: S3 bucket, Extension Layer, Lambda function
bin/
└── app.ts                     # CDK app entrypoint
scripts/
└── bootstrap                   # Extension entrypoint shell script
```

## Quick start

```bash
npm install
npm run cdk:deploy
```

That builds the extension with esbuild, synthesizes the CloudFormation stack, and deploys. The CDK stack creates:

- An S3 bucket (with auto-delete on destroy)
- A Lambda Layer containing the bundled extension
- A Node.js 20 Lambda function (ARM64, 512MB, 30s timeout) with the layer attached

After deploy, invoke the function with any event:

```bash
aws lambda invoke --function-name <function-name> response.json
```

Check the S3 bucket for your uploaded payload:

```bash
aws s3 ls s3://<bucket-name>/payloads/
```

## Commands

| Command | Does |
|---|---|
| `npm run build` | Build the extension (esbuild → `dist/extension/extensions/`) |
| `npm run cdk:synth` | Build + synth CloudFormation template |
| `npm run cdk:diff` | Build + show stack changes before deploying |
| `npm run cdk:deploy` | Build + deploy to AWS |
| `npm run cdk:destroy` | Tear down the stack (including the S3 bucket) |

## API

### `POST /upload`

The extension exposes a single endpoint on `localhost:{EXTENSION_PORT}` (default 4000).

| Header | Required | Purpose |
|---|---|---|
| `Content-Type` | No | Passed through to S3 `PutObject` |
| `Content-Length` | No* | Pre-flight body size check |
| `X-S3-Key` | No | Custom S3 object key (defaults to `uploads/{timestamp}-{uuid}.json`) |

\* If `Content-Length` exceeds 6MB, the request is rejected before reading the body.

**Response:**

```json
{ "status": "accepted", "key": "payloads/abc-123.json" }
```

Status codes: `202` accepted, `413` payload too large, `503` queue full, `500` read error.

### Lambda handler contract

The handler wraps the invocation event like this:

```json
{
  "event": { /* whatever came in */ },
  "context": {
    "requestId": "...",
    "functionName": "...",
    "functionVersion": "..."
  },
  "timestamp": "2025-01-01T00:00:00.000Z"
}
```

This gets uploaded to `s3://{bucket}/payloads/{requestId}.json`.

### Retry & durability

| Property | Value |
|---|---|
| Max retry cycles | 5 |
| Backoff | 1s → 2s → 4s → 8s → 16s → 30s (capped) |
| Recovery | Scans `/tmp/s3-queue/` on cold start, re-enqueues incomplete jobs |
| Abandoned jobs | Logged to stderr, files cleaned up — no dead letter queue |

### Extension lifecycle

The extension registers with the Lambda Runtime API for `INVOKE` and `SHUTDOWN` events. It does not intercept or inspect the handler's response — it's a pure sidecar. The handler and extension run concurrently; there's no coupling beyond the HTTP call.

On `SHUTDOWN`, the extension stops accepting new requests, closes the HTTP server, and polls active uploads for up to 2 seconds before calling `process.exit(0)`.

## Configurable constants

All tunable values live at the top of `src/extension/index.ts`:

| Constant | Default | Purpose |
|---|---|---|
| `PORT` | 4000 | HTTP server port (env: `EXTENSION_PORT`) |
| `MAX_UPLOADS_CONCURRENT` | 5 | Concurrent S3 uploads |
| `MAX_BODY_SIZE` | 6 MB | Max request body |
| `MAX_QUEUED_JOBS` | 1000 | Max pending jobs |
| `MAX_RETRY_CYCLES` | 5 | Max retry attempts per job |
| `SHUTDOWN_DEADLINE_MS` | 2000 | Max drain time on shutdown |
| `REQUEST_TIMEOUT_MS` | 10000 | HTTP request timeout |

## No local dev server

There's no local emulation. The extension depends on the Lambda Runtime API and `/tmp/` filesystem semantics. You test by deploying. Run `npm run cdk:deploy` and invoke the function.

## Teardown

```bash
npm run cdk:destroy
```

Destroys the entire stack, including the S3 bucket and all uploaded payloads.

## License

MIT
