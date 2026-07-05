import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';

const PORT = parseInt(process.env.EXTENSION_PORT || '4000', 10);
const BUCKET = process.env.BUCKET_NAME || '';
const RUNTIME_API = process.env.AWS_LAMBDA_RUNTIME_API || '';
const EXTENSION_NAME = 's3-upload-extension';
const QUEUE_DIR = '/tmp/s3-queue';
const MAX_UPLOADS_CONCURRENT = 5;
const MAX_BODY_SIZE = 6 * 1024 * 1024;
const MAX_QUEUED_JOBS = 1000;
const MAX_RETRY_CYCLES = 5;
const SHUTDOWN_DEADLINE_MS = 2000;
const REQUEST_TIMEOUT_MS = 10_000;

const s3 = new S3Client({ maxAttempts: 3 });

fs.mkdirSync(QUEUE_DIR, { recursive: true });

type Job = {
  filename: string;
  key: string;
  contentType?: string;
  retries: number;
};

const queue: Job[] = [];
let activeUploads = 0;

function jsonResponse(res: http.ServerResponse, status: number, body: Record<string, unknown>): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function requireEnv(name: string, value: string): void {
  if (!value) {
    console.error(`[ext] ${name} environment variable is required`);
    process.exit(1);
  }
}

function jobPath(filename: string): string {
  return path.join(QUEUE_DIR, filename);
}

function metaPath(base: string): string {
  return path.join(QUEUE_DIR, `${base}.json`);
}

function bodyPath(base: string): string {
  return path.join(QUEUE_DIR, `${base}.bin`);
}

function createJobId(): string {
  return randomUUID();
}

function createS3Key(requestKey?: string): string {
  return requestKey || `uploads/${Date.now()}-${createJobId()}.json`;
}

function removeFile(filePath: string): void {
  try { fs.unlinkSync(filePath); } catch { /* already gone */ }
}

function removeJobFiles(base: string): void {
  removeFile(metaPath(base));
  removeFile(bodyPath(base));
}

async function uploadToS3(key: string, body: Buffer, contentType?: string): Promise<void> {
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: body,
      ContentType: contentType || 'application/octet-stream',
    }),
  );
  console.log(`[ext] uploaded: ${key}`);
}

function writeJobToDisk(base: string, key: string, body: Buffer, contentType?: string): void {
  fs.writeFileSync(bodyPath(base), body);
  fs.writeFileSync(metaPath(base), JSON.stringify({ key, contentType, retries: 0 }));
}

function readJobFromDisk(base: string): Job | null {
  const metaFile = metaPath(base);

  if (!fs.existsSync(bodyPath(base))) {
    removeFile(metaFile);
    return null;
  }

  try {
    const raw = fs.readFileSync(metaFile, 'utf-8');
    const parsed = JSON.parse(raw) as { key: string; contentType?: string; retries?: number };
    return {
      filename: base,
      key: parsed.key,
      contentType: parsed.contentType,
      retries: parsed.retries ?? 0,
    };
  } catch {
    removeJobFiles(base);
    return null;
  }
}

function readJobBody(base: string): Buffer | null {
  try {
    return fs.readFileSync(bodyPath(base));
  } catch {
    removeFile(metaPath(base));
    return null;
  }
}

function persistRetries(job: Job): void {
  try {
    fs.writeFileSync(
      metaPath(job.filename),
      JSON.stringify({ key: job.key, contentType: job.contentType, retries: job.retries }),
    );
  } catch { /* best effort */ }
}

function listMetaFiles(): string[] {
  return fs
    .readdirSync(QUEUE_DIR)
    .filter((f) => f.endsWith('.json'))
    .sort((a, b) => {
      try {
        return fs.statSync(jobPath(a)).birthtimeMs - fs.statSync(jobPath(b)).birthtimeMs;
      } catch {
        return 0;
      }
    });
}

function listBodyFiles(): Set<string> {
  return new Set(
    fs.readdirSync(QUEUE_DIR)
      .filter((f) => f.endsWith('.bin'))
      .map((f) => f.replace(/\.bin$/, '')),
  );
}

function loadQueueFromDisk(): void {
  const bodyFiles = listBodyFiles();

  for (const file of listMetaFiles()) {
    const base = file.replace(/\.json$/, '');
    bodyFiles.delete(base);

    const job = readJobFromDisk(base);
    if (job && job.retries < MAX_RETRY_CYCLES) {
      queue.push(job);
    } else if (job) {
      removeJobFiles(base);
    }
  }

  for (const orphan of bodyFiles) {
    removeFile(bodyPath(orphan));
  }
}

function enqueueJob(base: string, key: string, contentType?: string): void {
  queue.push({ filename: base, key, contentType, retries: 0 });
}

function acceptJob(res: http.ServerResponse, key: string): void {
  jsonResponse(res, 202, { status: 'accepted', key });
}

function rejectJob(res: http.ServerResponse, status: number, reason: string): void {
  jsonResponse(res, status, { status: 'rejected', reason });
}

function isBodyTooLarge(contentLength: string | undefined): boolean {
  return parseInt(contentLength || '0', 10) > MAX_BODY_SIZE;
}

function isQueueFull(): boolean {
  return queue.length >= MAX_QUEUED_JOBS;
}

function readRequestBody(req: http.IncomingMessage, onComplete: (body: Buffer) => void): void {
  req.setTimeout(REQUEST_TIMEOUT_MS);
  req.on('timeout', () => req.destroy());

  let rejected = false;
  const chunks: Buffer[] = [];
  let size = 0;

  req.on('data', (chunk: Buffer) => {
    if (rejected) return;
    size += chunk.length;
    if (size > MAX_BODY_SIZE) {
      rejected = true;
      req.destroy();
      return;
    }
    chunks.push(chunk);
  });

  req.on('end', () => {
    if (!rejected) onComplete(Buffer.concat(chunks));
  });
}

function handleUploadRequest(req: http.IncomingMessage, res: http.ServerResponse): void {
  if (isBodyTooLarge(req.headers['content-length'])) {
    rejectJob(res, 413, 'payload too large');
    return;
  }

  readRequestBody(req, (body) => {
    if (!body.length && isBodyTooLarge(req.headers['content-length'])) {
      rejectJob(res, 413, 'payload too large');
      return;
    }

    if (isQueueFull()) {
      rejectJob(res, 503, 'queue full');
      return;
    }

    const base = createJobId();
    const key = createS3Key(req.headers['x-s3-key'] as string | undefined);
    const contentType = req.headers['content-type'];

    writeJobToDisk(base, key, body, contentType || undefined);
    enqueueJob(base, key, contentType || undefined);
    acceptJob(res, key);
    pumpQueue();
  });

  req.on('error', () => {
    if (!res.headersSent) rejectJob(res, 500, 'read error');
  });
}

function createServer(): http.Server {
  return http.createServer((req, res) => {
    if (req.method === 'POST' && req.url === '/upload') {
      handleUploadRequest(req, res);
    } else {
      res.writeHead(404);
      res.end();
    }
  });
}

function pumpQueue(): void {
  while (activeUploads < MAX_UPLOADS_CONCURRENT && queue.length > 0) {
    const job = queue.shift()!;

    const body = readJobBody(job.filename);
    if (!body) continue;

    const cycle = job.retries + 1;
    activeUploads++;

    uploadToS3(job.key, body, job.contentType)
      .then(() => removeJobFiles(job.filename))
      .catch((err) => handleUploadFailure(job, cycle, err))
      .finally(() => {
        activeUploads--;
        pumpQueue();
      });
  }
}

function handleUploadFailure(job: Job, cycle: number, error: unknown): void {
  if (cycle >= MAX_RETRY_CYCLES) {
    console.error(`[ext] abandoned: ${job.key}`);
    removeJobFiles(job.filename);
    return;
  }

  console.error(`[ext] cycle ${cycle}/${MAX_RETRY_CYCLES} failed: ${job.key}`, error);
  const retryJob: Job = { ...job, retries: cycle };
  persistRetries(retryJob);

  const delay = Math.min(1000 * 2 ** (cycle - 1), 30000);
  setTimeout(() => {
    queue.push(retryJob);
    pumpQueue();
  }, delay);
}

function handleShutdown(server: http.Server): void {
  console.log(`[ext] shutdown (${queue.length} queued, ${activeUploads} active)`);
  server.closeAllConnections?.();
  server.close();
  pumpQueue();
}

function waitForActiveUploads(): Promise<void> {
  const deadline = Date.now() + SHUTDOWN_DEADLINE_MS;

  return new Promise((resolve) => {
    function poll(): void {
      if (activeUploads === 0 || Date.now() >= deadline) {
        resolve();
        return;
      }
      setTimeout(poll, 100);
    }
    poll();
  });
}

async function registerExtension(): Promise<string> {
  const res = await fetch(`http://${RUNTIME_API}/2020-01-01/extension/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Lambda-Extension-Name': EXTENSION_NAME },
    body: JSON.stringify({ events: ['INVOKE', 'SHUTDOWN'] }),
  });
  const { extensionId } = (await res.json()) as { extensionId: string };
  return extensionId;
}

async function fetchNextEvent(extensionId: string): Promise<{ eventType: string }> {
  const res = await fetch(`http://${RUNTIME_API}/2020-01-01/extension/event/next`, {
    headers: { 'Lambda-Extension-Identifier': extensionId },
  });
  return res.json() as Promise<{ eventType: string }>;
}

async function extensionEventLoop(extensionId: string, server: http.Server): Promise<void> {
  while (true) {
    let event: { eventType: string };
    try {
      event = await fetchNextEvent(extensionId);
    } catch (err) {
      console.error('[ext] event fetch failed, retrying...', err);
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    if (event.eventType === 'SHUTDOWN') {
      handleShutdown(server);
      await waitForActiveUploads();
      console.log(`[ext] exiting (${queue.length} queued, ${activeUploads} active)`);
      process.exit(0);
    }
  }
}

async function main(): Promise<void> {
  requireEnv('BUCKET_NAME', BUCKET);
  requireEnv('AWS_LAMBDA_RUNTIME_API', RUNTIME_API);

  loadQueueFromDisk();
  if (queue.length > 0) {
    console.log(`[ext] recovered ${queue.length} job(s) from disk`);
  }
  pumpQueue();

  const server = createServer();
  await new Promise<void>((resolve) => server.listen(PORT, resolve));
  console.log(`[ext] listening :${PORT}`);

  const extensionId = await registerExtension();
  console.log(`[ext] registered ${extensionId}`);

  await extensionEventLoop(extensionId, server);
}

main().catch((err) => {
  console.error('[ext] fatal:', err);
  process.exit(1);
});
