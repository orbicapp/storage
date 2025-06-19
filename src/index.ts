import { zValidator } from "@hono/zod-validator";
import { Context, Hono, Next } from "hono";
import { cors } from "hono/cors";
import { sign, verify } from "hono/jwt";
import { z } from "zod";

// Environment types
interface Env {
  BUCKET: R2Bucket;
  KV: KVNamespace;
  JWT_SECRET: string;
  [key: string]: unknown;
}

// Validation schemas with Zod
const CreateUploadSchema = z.object({
  id: z.string().min(1),
  fileSize: z.number().positive(),
  mimeType: z.string().min(1),
});

const CompleteUploadSchema = z.object({
  uploadId: z.string().min(1),
  parts: z.array(
    z.object({
      partNumber: z.number().positive(),
      etag: z.string().min(1),
    })
  ),
});

// JWT Payload types
interface BackendJWTPayload {
  type: "backend";
  action: "create" | "complete" | "abort";
  iat?: number;
  exp?: number;
}

interface ClientJWTPayload {
  type: "client";
  uploadId: string;
  fileId: string;
  maxFileSize: number;
  mimeType: string;
  iat?: number;
  exp?: number;
}

const app = new Hono<{ Bindings: Env }>();

// Cors
app.use(
  "*",
  cors({
    origin: (origin: string) => {
      return origin;
    },
    credentials: true,
  })
);

// Middleware to validate backend JWT
const backendAuth = async (
  c: Context<{
    Bindings: Env;
    Variables: { user: BackendJWTPayload | ClientJWTPayload };
  }>,
  next: Next
) => {
  try {
    const authHeader = c.req.header("Authorization");

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return c.json(
        { success: false, error: "Missing or invalid authorization header" },
        401
      );
    }

    const token = authHeader.substring(7);
    const payload = (await verify(
      token,
      c.env.JWT_SECRET
    )) as BackendJWTPayload;

    if (!payload || payload.type !== "backend") {
      return c.json({ success: false, error: "Invalid token type" }, 401);
    }

    // Check expiration
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
      return c.json({ success: false, error: "Token expired" }, 401);
    }

    c.set("user", payload);
    return await next();
  } catch (error) {
    return c.json(
      { success: false, error: "Invalid token (Back): " + error },
      401
    );
  }
};

// Middleware to validate client JWT
const clientAuth = async (
  c: Context<{
    Bindings: Env;
    Variables: { user: BackendJWTPayload | ClientJWTPayload };
  }>,
  next: Next
) => {
  try {
    const authHeader = c.req.header("Authorization");

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return c.json(
        { success: false, error: "Missing or invalid authorization header" },
        401
      );
    }

    const token = authHeader.substring(7);
    const payload = (await verify(token, c.env.JWT_SECRET)) as ClientJWTPayload;

    if (!payload || payload.type !== "client") {
      return c.json({ success: false, error: "Invalid token type" }, 401);
    }

    // Check expiration
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
      return c.json({ success: false, error: "Token expired" }, 401);
    }

    c.set("user", payload);
    return await next();
  } catch (error) {
    return c.json(
      { success: false, error: "Invalid token (Client): " + error },
      401
    );
  }
};

// Utilities
const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB
const KV_PREFIX = "upload_progress:";

async function getUploadProgress(
  kv: KVNamespace,
  uploadId: string
): Promise<number> {
  const progress = await kv.get(`${KV_PREFIX}${uploadId}`);
  return progress ? parseInt(progress) : 0;
}

async function updateUploadProgress(
  kv: KVNamespace,
  uploadId: string,
  bytes: number
): Promise<void> {
  await kv.put(`${KV_PREFIX}${uploadId}`, bytes.toString());
}

async function deleteUploadProgress(
  kv: KVNamespace,
  uploadId: string
): Promise<void> {
  await kv.delete(`${KV_PREFIX}${uploadId}`);
}

// Backend routes (with backend JWT)

// 1. Create multipart upload
app.post(
  "/upload/create",
  backendAuth,
  zValidator("json", CreateUploadSchema),
  async (c) => {
    try {
      const { id, fileSize, mimeType } = c.req.valid("json");
      const bucket = c.env.BUCKET;

      // Create multipart upload in R2
      const multipartUpload = await bucket.createMultipartUpload(id, {
        httpMetadata: {
          contentType: mimeType,
        },
      });

      // Calculate number of required parts
      const totalParts = Math.ceil(fileSize / CHUNK_SIZE);

      // Generate JWT for client
      const clientToken = await sign(
        {
          type: "client",
          uploadId: multipartUpload.uploadId,
          fileId: id,
          maxFileSize: fileSize,
          mimeType,
          exp: Math.floor(Date.now() / 1000) + 60 * 60 * 2, // 2 hours
        } as ClientJWTPayload,
        c.env.JWT_SECRET
      );

      // Initialize progress in KV
      await updateUploadProgress(c.env.KV, multipartUpload.uploadId, 0);

      return c.json({
        success: true,
        uploadId: multipartUpload.uploadId,
        fileId: id,
        totalParts,
        chunkSize: CHUNK_SIZE,
        clientToken,
      });
    } catch (error: any) {
      return c.json(
        {
          success: false,
          error: error.message,
        },
        500
      );
    }
  }
);

// 2. Complete multipart upload
app.post(
  "/upload/complete",
  backendAuth,
  zValidator("json", CompleteUploadSchema),
  async (c) => {
    try {
      const { uploadId, parts } = c.req.valid("json");
      const bucket = c.env.BUCKET;

      // Get key from uploadId (you would need to store this relationship)
      // For simplicity, we assume uploadId contains key info
      const fileId = c.req.query("fileId");
      if (!fileId) {
        return c.json({ success: false, error: "Missing fileId" }, 400);
      }

      const multipartUpload = bucket.resumeMultipartUpload(fileId, uploadId);

      const object = await multipartUpload.complete(parts);

      // Clean progress from KV
      await deleteUploadProgress(c.env.KV, uploadId);

      return c.json({
        success: true,
        etag: object.httpEtag,
        size: object.size,
      });
    } catch (error: any) {
      return c.json(
        {
          success: false,
          error: error.message,
        },
        500
      );
    }
  }
);

// 3. Abort multipart upload
app.delete("/upload/abort/:uploadId", backendAuth, async (c) => {
  try {
    const uploadId = c.req.param("uploadId");
    const fileId = c.req.query("fileId");

    if (!fileId) {
      return c.json({ success: false, error: "Missing fileId" }, 400);
    }

    const bucket = c.env.BUCKET;
    const multipartUpload = bucket.resumeMultipartUpload(fileId, uploadId);

    await multipartUpload.abort();

    // Clean progress from KV
    await deleteUploadProgress(c.env.KV, uploadId);

    return c.json({ success: true });
  } catch (error: any) {
    return c.json(
      {
        success: false,
        error: error.message,
      },
      500
    );
  }
});

// 4. Delete file
app.delete("/file/:fileId", backendAuth, async (c) => {
  try {
    const fileId = c.req.param("fileId");
    const bucket = c.env.BUCKET;

    // Check if file exists
    const object = await bucket.head(fileId);

    if (!object) {
      return c.json(
        {
          success: false,
          error: "File not found",
        },
        404
      );
    }

    // Delete file
    await bucket.delete(fileId);

    // Delete KV garbage
    try {
      await c.env.KV.delete(`${KV_PREFIX}${fileId}`);
    } catch (kvError) {
      console.warn(`Could not delete KV entry for ${fileId}:`, kvError);
    }

    return c.json({
      success: true,
      message: `File ${fileId} deleted successfully`,
    });
  } catch (error: any) {
    console.error(`Error deleting file ${c.req.param("fileId")}:`, error);
    return c.json(
      {
        success: false,
        error: error.message || "Failed to delete file",
      },
      500
    );
  }
});

// Client routes (with client JWT)

// 5. Upload file part
app.put("/upload/part/:partNumber", clientAuth, async (c) => {
  try {
    const partNumber = parseInt(c.req.param("partNumber"));
    const payload = c.get("user") as ClientJWTPayload;

    if (!payload || payload.type !== "client") {
      return c.json(
        { success: false, error: "Invalid token (Client was expected)" },
        401
      );
    }

    const body = await c.req.arrayBuffer();

    if (!body || body.byteLength === 0) {
      return c.json({ success: false, error: "Missing request body" }, 400);
    }

    // Validate chunk size
    const isLastPart = c.req.query("isLast") === "true";
    if (!isLastPart && body.byteLength < CHUNK_SIZE) {
      return c.json(
        {
          success: false,
          error: `Chunk must be at least ${CHUNK_SIZE} bytes, except for the last part`,
        },
        400
      );
    }

    // Verify current progress
    const currentProgress = await getUploadProgress(c.env.KV, payload.uploadId);
    const expectedBytes = (partNumber - 1) * CHUNK_SIZE;

    if (currentProgress !== expectedBytes) {
      return c.json(
        {
          success: false,
          error: "Invalid part sequence",
        },
        400
      );
    }

    const bucket = c.env.BUCKET;
    const multipartUpload = bucket.resumeMultipartUpload(
      payload.fileId,
      payload.uploadId
    );

    const uploadedPart = await multipartUpload.uploadPart(partNumber, body);

    // Update progress
    const newProgress = currentProgress + body.byteLength;
    await updateUploadProgress(c.env.KV, payload.uploadId, newProgress);

    return c.json({
      success: true,
      partNumber: uploadedPart.partNumber,
      etag: uploadedPart.etag,
      uploadedBytes: newProgress,
      totalBytes: payload.maxFileSize,
    });
  } catch (error: any) {
    return c.json(
      {
        success: false,
        error: error.message,
      },
      500
    );
  }
});

// 6. Get upload progress
app.get("/upload/progress", clientAuth, async (c) => {
  try {
    const payload = c.get("user") as ClientJWTPayload;

    if (!payload || payload.type !== "client") {
      return c.json(
        { success: false, error: "Invalid token (Client was expected)" },
        401
      );
    }

    const uploadedBytes = await getUploadProgress(c.env.KV, payload.uploadId);
    const progress = (uploadedBytes / payload.maxFileSize) * 100;

    return c.json({
      success: true,
      uploadedBytes,
      totalBytes: payload.maxFileSize,
      progress: Math.round(progress * 100) / 100,
    });
  } catch (error: any) {
    return c.json(
      {
        success: false,
        error: error.message,
      },
      500
    );
  }
});

// 7. Serve files (public)
app.get("/file/:fileId", async (c) => {
  try {
    const fileId = c.req.param("fileId");
    const bucket = c.env.BUCKET;

    const object = await bucket.get(fileId);

    if (!object) {
      return c.json({ error: "File not found" }, 404);
    }

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("etag", object.httpEtag);

    // Add cache headers
    headers.set("Cache-Control", "public, max-age=31536000");

    return new Response(object.body, { headers });
  } catch (error: any) {
    return c.json(
      {
        success: false,
        error: error.message,
      },
      500
    );
  }
});

// Health check route
app.get("/health", (c) => {
  return c.json({ status: "ok", timestamp: new Date().toISOString() });
});

// 404 error handling
app.notFound((c) => {
  return c.json({ error: "Not Found" }, 404);
});

// General error handling
app.onError((err, c) => {
  console.error("Worker error:", err);
  return c.json(
    {
      success: false,
      error: "Internal Server Error",
    },
    500
  );
});

export default app;
