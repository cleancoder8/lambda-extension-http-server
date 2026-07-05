# lambda-extension-http-server

An AWS CDK application that deploys an AWS Lambda function with a sidecar Lambda Extension providing async, durable S3 uploads via an internal HTTP server. The Lambda handler POSTs payloads to the extension's local HTTP server and immediately returns — the extension handles S3 uploads asynchronously with retries, queue recovery on cold starts, and graceful shutdown.

## Project structure

```
bin/app.ts                         # CDK app entrypoint — creates the stack
lib/lambda-extension-stack.ts      # CDK stack: S3 bucket, Lambda Layer, Lambda function
src/lambda/index.ts                # Lambda function handler — POSTs to extension's /upload
src/extension/index.ts             # Lambda Extension — HTTP server + S3 upload queue
scripts/bootstrap                  # Extension bootstrap — exec node /opt/extensions/index.js
dist/extension/extensions/         # Build output: index.js + bootstrap (gitignored)
cdk.out/                           # CDK synthesis artifacts (gitignored)
```

## Architecture

```
Lambda handler ──HTTP POST localhost:4000/upload──▶ Extension HTTP server
                                                      │
                                                      ├─ Persists body → /tmp/s3-queue/{uuid}.bin
                                                      ├─ Responds HTTP 202 immediately
                                                      ├─ Async upload to S3 (5 concurrent, exp backoff retry)
                                                      ├─ Recovers incomplete jobs from /tmp on cold start
                                                      └─ Drains queue on SHUTDOWN (2-second deadline)
```

The Lambda handler (`src/lambda/index.ts`) receives any event, wraps it with context (requestId, functionName, functionVersion) and timestamp, and POSTs to `http://localhost:4000/upload` with an `X-S3-Key` header. Retries up to 3 attempts with AbortController (2s timeout) and linear backoff (100ms/200ms/300ms). Returns 200 on success, 502 on failure.

The extension (`src/extension/index.ts`) runs as a Linux process inside the Lambda execution environment. It registers with the Lambda Runtime API, subscribes to INVOKE and SHUTDOWN events, and starts an HTTP server on the port specified by `EXTENSION_PORT` (default 4000). The `/upload` endpoint validates body size (max 6MB), persists to `/tmp/s3-queue/`, responds HTTP 202, and pumps an async upload queue. Uploads are processed 5 at a time with exponential backoff retry (1s → 2s → 4s → 8s → 16s → 30s max, up to 5 total attempts). Failed uploads that exhaust retries are abandoned (logged to stderr). On SHUTDOWN, the extension stops the HTTP server, drains active uploads (2-second deadline), then exits.

## Commands

| Command | What it does |
|---|---|
| `npm run build` | Alias for `build:extension` |
| `npm run build:extension` | esbuild bundles `src/extension/index.ts` → `dist/extension/extensions/index.js` (Node 18, external `@aws-sdk/*`), copies `scripts/bootstrap` and `chmod +x` it |
| `npm run cdk:synth` | Build extension then `cdk synth` → CloudFormation in `cdk.out/` |
| `npm run cdk:deploy` | Build extension then `cdk deploy` |
| `npm run cdk:diff` | Build extension then `cdk diff` (shows stack changes before deploy) |
| `npm run cdk:destroy` | `cdk destroy` (tears down the entire stack, including the S3 bucket) |

**There is no `npm test`, `npm run lint`, or `npm run format`.** No test framework (jest/vitest) or linter (eslint) is configured.

To verify the build works locally: `npm run build:extension` — check that `dist/extension/extensions/index.js` and `dist/extension/extensions/bootstrap` exist and that bootstrap is executable.

## Tech stack

- **Runtime:** Node.js 18 (extension), Node.js 20 (Lambda function)
- **Language:** TypeScript 5.4, strict mode, ES2022 target, CommonJS modules
- **Bundler:** esbuild 0.23 for the extension (needs to be a single self-contained JS file in the Lambda Layer)
- **Infra:** AWS CDK v2 with `aws-cdk-lib` and `constructs`
- **AWS SDK:** `@aws-sdk/client-s3` v3 for S3 PutObject — externalized from esbuild bundle because it's available in the Node.js Lambda runtime
- **Running locally:** There is **no local dev server or emulation**. The extension can only run inside a real Lambda execution environment. Test by deploying to AWS.

## Conventions

- All source TypeScript lives in `src/` — `src/extension/` for the extension, `src/lambda/` for the Lambda handler
- CDK infra lives in `lib/` and `bin/` — standard CDK convention
- The extension lives in the Lambda Layer at `/opt/extensions/` — this follows the [AWS Lambda Extension API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html). The `bootstrap` shell script is the entrypoint that the Lambda runtime invokes to start the extension.
- `/tmp/` is the only writable directory in Lambda — used for the S3 upload queue (`/tmp/s3-queue/`)
- S3 keys default to `uploads/{timestamp}-{uuid}.json` unless the `X-S3-Key` header is set. The Lambda handler sets it to `payloads/{requestId}.json`.
- `@aws-sdk/*` packages are externalized from the esbuild bundle — don't add runtime deps, put them in devDependencies
- The extension bundle is a single `.js` file; no node_modules needed in the layer
- No `console.log` → use stderr for logging in the extension (stdout is reserved for the Lambda Runtime API)
- Constants at the top of `src/extension/index.ts` are the source of truth for tuning: `PORT`, `MAX_UPLOADS_CONCURRENT`, `MAX_BODY_SIZE`, `MAX_QUEUED_JOBS`, `MAX_RETRY_CYCLES`, `SHUTDOWN_DEADLINE_MS`, `REQUEST_TIMEOUT_MS`

## Extension lifecycle (important detail)

The Lambda Extension protocol works like this:
1. Extension starts (via `bootstrap` shell script)
2. Registers with `POST /2020-01-01/extension/register` — subscribes to INVOKE and SHUTDOWN events
3. Enters an event loop: `GET /2020-01-01/extension/event/next` — blocks until next lifecycle event
4. On INVOKE: the Lambda handler runs concurrently with the extension
5. On SHUTDOWN: the extension has ~2 seconds to clean up before the environment is frozen/destroyed

The extension **does not know** what the Lambda handler is doing — it just provides an HTTP endpoint that the handler hits. This is a "sidecar" pattern, not a middleware/interceptor pattern.

## File manifest (important for LLMs)

- `package.json:21` — `build:extension` script with exact esbuild flags
- `src/extension/index.ts:1-380` — full extension: HTTP server, S3 upload queue, Lambda Extension protocol, disk persistence, retry logic
- `src/lambda/index.ts:1-88` — Lambda handler: event → HTTP POST to extension
- `lib/lambda-extension-stack.ts:1-100` — CDK stack: S3 bucket, layer, function, IAM permissions
- `scripts/bootstrap:1-3` — simple shell script that execs the extension
- `bin/app.ts:1-12` — CDK app entrypoint (boilerplate)
- `tsconfig.json:1-25` — TypeScript config: strict, ES2022, CommonJS
- `cdk.json:1-20` — CDK entrypoint and watch settings
