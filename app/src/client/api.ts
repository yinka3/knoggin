"use client";

import type { ApiError } from "@/contracts/api";

const apiBaseUrl = (
  process.env.NEXT_PUBLIC_KNOGGIN_API_BASE_URL ?? "http://127.0.0.1:8000"
).replace(/\/$/, "");

export class KnogginApiError extends Error {
  constructor(
    public readonly error: ApiError,
    public readonly status: number,
  ) {
    super(error.message);
  }
}

export async function apiRequest<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...init,
    headers: {
      Accept: "application/json",
      ...init.headers,
    },
  });
  if (!response.ok) {
    const payload = (await response.json().catch(() => ({}))) as {
      error?: ApiError;
    };
    throw new KnogginApiError(
      payload.error ?? {
        code: "API_REQUEST_FAILED",
        message: "The API request could not be completed.",
        retryable: response.status >= 500,
      },
      response.status,
    );
  }
  return (await response.json()) as T;
}

export { apiBaseUrl };
