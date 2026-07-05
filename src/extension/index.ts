import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';

const PORT = parseInt(process.env.EXTENSION_PORT || '4000', 10);
const BUCKET = process.env.BUCKET_NAME!;
const RUNTIME_API = process.env.AWS_LAMBDA_RUNTIME_API!;
const NAME = 's3-upload-extension';
const QUEUE_DIR = '/tmp/s3-queue';
const MAX_BODY = 6 * 1024 * 1024;
const MAX_CONCURRENT = 5;
const MAX_QUEUED = 1000;
const MAX_RETRY_CYCLES = 5;
const SHUTDOWN_DEADLINE_MS = 2000;
const REQUEST_TIMEOUT_MS = 10_000;

const s3 = new S3Client({ maxAttempts: 3 });

function jsonReply(
  res: http.ServerResponse,
  status: number,
  body: Record<string, unknown>,
): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

fs.mkdirSync(QUEUE_DIR, { recursive: true });

interface Job {
  filename: string;
  key: string;
  contentType?: string;
  retries: number;
}

const jobQueue: Job[] = [];
let inflight = 0;

function loadQueueFromDisk(): void {
  const processed = new Set<string>();

  const files = fs
    .readdirSync(QUEUE_DIR)
    .filter((f) => f.endsWith('.json'))
    .sort((a, b) => {
      try {
        return (
          fs.statSync(path.join(QUEUE_DIR, a)).birthtimeMs -
          fs.statSync(path.join(QUEUE_DIR, b)).birthtimeMs
        );
      } catch {
        return 0;
      }
    });

  for (const file of files) {
    const base = file.replace(/\.json$/, '');
    const metaPath = path.join(QUEUE_DIR, file);
    const bodyPath = path.join(QUEUE_DIR, `${base}.bin`);

    if (!fs.existsSync(bodyPath)) {
      try { fs.unlinkSync(metaPath); } catch { /* ignore */ }
      continue;
    }

    try {
      const meta = JSON.parse(
        fs.readFileSync(metaPath, 'utf-8'),
      ) as { key: string; contentType?: string; retries?: number };

      const retries = meta.retries ?? 0;
      if (retries >= MAX_RETRY_CYCLES) {
        try {
          fs.unlinkSync(metaPath);
          fs.unlinkSync(bodyPath);
        } catch {
          /* ignore */
        }
        continue;
      }

      jobQueue.push({
        filename: base,
        key: meta.key,
        contentType: meta.contentType,
        retries,
      });
      processed.add(base);
    } catch {
      try {
        fs.unlinkSync(metaPath);
        try { fs.unlinkSync(bodyPath); } catch { /* ignore */ }
      } catch {
        /* ignore */
      }
    }
  }

  for (const file of fs.readdirSync(QUEUE_DIR).filter((f) => f.endsWith('.bin'))) {
    const base = file.replace(/\.bin$/, '');
    if (!processed.has(base)) {
      try { fs.unlinkSync(path.join(QUEUE_DIR, file)); } catch { /* ignore */ }
    }
  }
}

function persistRetries(job: Job): void {
  const metaPath = path.join(QUEUE_DIR, `${job.filename}.json`);
  try {
    fs.writeFileSync(
      metaPath,
      JSON.stringify({
        key: job.key,
        contentType: job.contentType,
        retries: job.retries,
      }),
    );
  } catch {
    /* can't write — continue anyway */
  }
}

async function uploadToS3(
  key: string,
  body: Buffer,
  contentType?: string,
): Promise<void> {
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

function drain(): void {
  const pump = (): void => {
    while (inflight < MAX_CONCURRENT && jobQueue.length > 0) {
      const job = jobQueue.shift()!;
      const bodyPath = path.join(QUEUE_DIR, `${job.filename}.bin`);
      const metaPath = path.join(QUEUE_DIR, `${job.filename}.json`);
      const cycle = job.retries + 1;

      let body: Buffer;
      try {
        body = fs.readFileSync(bodyPath);
      } catch {
        try { fs.unlinkSync(metaPath); } catch { /* ignore */ }
        continue;
      }

      inflight++;
      uploadToS3(job.key, body, job.contentType)
        .then(() => {
          try {
            fs.unlinkSync(bodyPath);
            fs.unlinkSync(metaPath);
          } catch {
            /* ignore */
          }
        })
        .catch((err) => {
          if (cycle >= MAX_RETRY_CYCLES) {
            console.error(`[ext] abandoned: ${job.key}`);
            try {
              fs.unlinkSync(bodyPath);
              fs.unlinkSync(metaPath);
            } catch {
              /* ignore */
            }
          } else {
            console.error(
              `[ext] cycle ${cycle}/${MAX_RETRY_CYCLES} failed: ${job.key}`,
              err,
            );
            const retryJob: Job = { ...job, retries: cycle };
            persistRetries(retryJob);
            const delay = Math.min(1000 * 2 ** (cycle - 1), 30000);
            setTimeout(() => {
              jobQueue.push(retryJob);
              drain();
            }, delay);
          }
        })
        .finally(() => {
          inflight--;
          pump();
        });
    }
  };

  pump();
}

const server = http.createServer((req, res) => {
  if (req.method !== 'POST' || req.url !== '/upload') {
    res.writeHead(404);
    res.end();
    return;
  }

  const contentLength = parseInt(req.headers['content-length'] || '0', 10);
  if (contentLength > MAX_BODY) {
    jsonReply(res, 413, { status: 'rejected', reason: 'payload too large' });
    return;
  }

  req.setTimeout(REQUEST_TIMEOUT_MS);
  req.on('timeout', () => {
    req.destroy();
  });
  req.on('error', () => {
    if (!res.headersSent) {
      jsonReply(res, 500, { status: 'rejected', reason: 'read error' });
    }
  });

  let rejected = false;
  const chunks: Buffer[] = [];
  let size = 0;

  req.on('data', (chunk: Buffer) => {
    size += chunk.length;
    if (size > MAX_BODY) {
      rejected = true;
      if (!res.headersSent) {
        jsonReply(res, 413, { status: 'rejected', reason: 'payload too large' });
      }
      req.destroy();
      return;
    }
    chunks.push(chunk);
  });

  req.on('end', () => {
    if (rejected) return;

    if (jobQueue.length >= MAX_QUEUED) {
      jsonReply(res, 503, { status: 'rejected', reason: 'queue full' });
      return;
    }

    const key =
      (req.headers['x-s3-key'] as string) ||
      `uploads/${Date.now()}-${randomUUID()}.json`;

    const filename = randomUUID();
    const body = Buffer.concat(chunks);
    const contentType = req.headers['content-type'] || undefined;

    fs.writeFileSync(path.join(QUEUE_DIR, `${filename}.bin`), body);
    fs.writeFileSync(
      path.join(QUEUE_DIR, `${filename}.json`),
      JSON.stringify({ key, contentType, retries: 0 }),
    );

    jobQueue.push({ filename, key, contentType, retries: 0 });

    jsonReply(res, 202, { status: 'accepted', key });
    drain();
  });
});

(async () => {
  if (!BUCKET) {
    console.error('[ext] BUCKET_NAME environment variable is required');
    process.exit(1);
  }
  if (!RUNTIME_API) {
    console.error('[ext] AWS_LAMBDA_RUNTIME_API environment variable is required');
    process.exit(1);
  }

  loadQueueFromDisk();
  if (jobQueue.length > 0) {
    console.log(`[ext] loaded ${jobQueue.length} queued job(s) from disk`);
  }
  drain();

  await new Promise<void>((resolve) => server.listen(PORT, resolve));
  console.log(`[ext] listening :${PORT}`);

  const reg = await fetch(
    `http://${RUNTIME_API}/2020-01-01/extension/register`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Lambda-Extension-Name': NAME,
      },
      body: JSON.stringify({ events: ['INVOKE', 'SHUTDOWN'] }),
    },
  );
  const { extensionId } = (await reg.json()) as { extensionId: string };
  console.log(`[ext] registered ${extensionId}`);

  while (true) {
    let event: { eventType: string };
    try {
      const ev = await fetch(
        `http://${RUNTIME_API}/2020-01-01/extension/event/next`,
        { headers: { 'Lambda-Extension-Identifier': extensionId } },
      );
      event = (await ev.json()) as { eventType: string };
    } catch (err) {
      console.error('[ext] event fetch error, retrying...', err);
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    if (event.eventType === 'SHUTDOWN') {
      console.log(
        `[ext] shutdown, draining ${jobQueue.length + inflight} jobs...`,
      );
      server.closeAllConnections?.();
      server.close();
      drain();

      const deadline = Date.now() + SHUTDOWN_DEADLINE_MS;
      while (inflight > 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }

      console.log(
        `[ext] exiting (${jobQueue.length} queued, ${inflight} in-flight)`,
      );
      process.exit(0);
    }
  }
})().catch((err) => {
  console.error('[ext] fatal:', err);
  process.exit(1);
});
