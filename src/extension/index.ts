import http from 'node:http';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';

const PORT = parseInt(process.env.EXTENSION_PORT || '4000', 10);
const BUCKET = process.env.BUCKET_NAME!;
const RUNTIME_API = process.env.AWS_LAMBDA_RUNTIME_API!;
const NAME = 's3-upload-extension';

const s3 = new S3Client({});

http.createServer((req, res) => {
  if (req.method !== 'POST' || req.url !== '/upload') {
    res.writeHead(404);
    res.end();
    return;
  }

  const chunks: Buffer[] = [];
  req.on('data', (chunk: Buffer) => chunks.push(chunk));
  req.on('end', () => {
    const body = Buffer.concat(chunks);
    const key =
      (req.headers['x-s3-key'] as string) ||
      `uploads/${Date.now()}-${randomUUID()}.json`;
    const contentType = req.headers['content-type'];

    s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: body,
        ContentType: contentType || 'application/octet-stream',
      }),
    )
      .then(() => console.log(`[ext] uploaded: ${key}`))
      .catch((err) => console.error(`[ext] upload failed ${key}:`, err));

    res.writeHead(200);
    res.end(JSON.stringify({ status: 'accepted', key }));
  });
}).listen(PORT, () => console.log(`[ext] listening :${PORT}`));

async function register(): Promise<string> {
  const res = await fetch(`http://${RUNTIME_API}/2020-01-01/extension/register`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Lambda-Extension-Name': NAME,
    },
    body: JSON.stringify({ events: ['INVOKE', 'SHUTDOWN'] }),
  });
  const { extensionId } = await res.json() as { extensionId: string };
  return extensionId;
}

async function eventLoop(extensionId: string) {
  while (true) {
    const res = await fetch(
      `http://${RUNTIME_API}/2020-01-01/extension/event/next`,
      { headers: { 'Lambda-Extension-Identifier': extensionId } },
    );
    const event = await res.json() as { eventType: string };
    if (event.eventType === 'SHUTDOWN') {
      console.log('[ext] shutdown');
      process.exit(0);
    }
  }
}

register()
  .then((id) => {
    console.log(`[ext] registered ${id}`);
    eventLoop(id);
  })
  .catch((err) => {
    console.error('[ext] fatal:', err);
    process.exit(1);
  });
