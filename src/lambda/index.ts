const EXTENSION_PORT = parseInt(process.env.EXTENSION_PORT || '4000', 10);
const EXTENSION_URL = `http://localhost:${EXTENSION_PORT}`;

export const handler = async (event: unknown, context: { awsRequestId: string; functionName: string; functionVersion: string }): Promise<{ statusCode: number; body: string }> => {
  const payload = {
    event,
    context: {
      requestId: context.awsRequestId,
      functionName: context.functionName,
      functionVersion: context.functionVersion,
    },
    timestamp: new Date().toISOString(),
  };

  const s3Key = `payloads/${context.awsRequestId}.json`;

  const controller = new AbortController();
  setTimeout(() => controller.abort(), 1000);

  fetch(`${EXTENSION_URL}/upload`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-S3-Key': s3Key,
    },
    body: JSON.stringify(payload),
    signal: controller.signal,
  }).catch((err) => {
    console.error('[lambda] failed to send payload to extension:', err.message);
  });

  return {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Payload queued for S3 upload',
      requestId: context.awsRequestId,
      s3Key,
    }),
  };
};
