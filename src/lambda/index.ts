const EXTENSION_PORT = parseInt(process.env.EXTENSION_PORT || '4000', 10);
const EXTENSION_URL = `http://localhost:${EXTENSION_PORT}`;

const sendPayload = async (
  s3Key: string,
  body: string,
): Promise<{ ok: boolean; reason?: string }> => {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 2000);

      const res = await fetch(`${EXTENSION_URL}/upload`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-S3-Key': s3Key,
        },
        body,
        signal: controller.signal,
      });

      if (res.ok) return { ok: true };

      let reason = `extension error: ${res.status}`;
      try {
        const payload = (await res.json()) as { reason?: string };
        if (payload.reason) reason = payload.reason;
      } catch {
        /* can't parse body, use status-based reason */
      }

      return { ok: false, reason };
    } catch (err) {
      if (attempt < 3) {
        await new Promise((r) => setTimeout(r, 100 * attempt));
      } else {
        return {
          ok: false,
          reason: err instanceof Error ? err.message : 'extension unreachable',
        };
      }
    }
  }
  return { ok: false, reason: 'unreachable' };
}

export const handler = async (
  event: unknown,
  context: {
    awsRequestId: string;
    functionName: string;
    functionVersion: string;
  },
): Promise<{ statusCode: number; body: string }> => {
  const payload = JSON.stringify({
    event,
    context: {
      requestId: context.awsRequestId,
      functionName: context.functionName,
      functionVersion: context.functionVersion,
    },
    timestamp: new Date().toISOString(),
  });

  const s3Key = `payloads/${context.awsRequestId}.json`;
  const result = await sendPayload(s3Key, payload);

  if (result.ok) {
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Payload accepted for S3 upload',
        requestId: context.awsRequestId,
        s3Key,
      }),
    };
  }

  return {
    statusCode: 502,
    body: JSON.stringify({
      message: 'Payload rejected',
      requestId: context.awsRequestId,
      reason: result.reason,
    }),
  };
};
