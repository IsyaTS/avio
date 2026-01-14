import { BootstrapState } from './bootstrap';

export type ApiContext = {
  tenantId: number | null;
  key: string;
  urls: Record<string, string | undefined>;
  webhookSecret?: string;
};

export function buildUrl(
  path: string,
  ctx: ApiContext,
  params?: Record<string, string | number | boolean | undefined | null>
): string {
  const trimmed = (path || '').trim();
  const base = trimmed || '/';
  const url = base.startsWith('http')
    ? new URL(base)
    : new URL(base, window.location.origin);

  if (ctx.tenantId && !url.searchParams.get('tenant')) {
    url.searchParams.set('tenant', String(ctx.tenantId));
  }
  if (ctx.key && !url.searchParams.get('k')) {
    url.searchParams.set('k', ctx.key);
  }
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      url.searchParams.set(key, String(value));
    });
  }
  return url.toString();
}

export async function requestJson<T>(
  url: string,
  options: RequestInit = {}
): Promise<T> {
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function postJson<T>(
  url: string,
  payload: unknown,
  options: RequestInit = {}
): Promise<T> {
  return requestJson<T>(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    body: JSON.stringify(payload),
    ...options,
  });
}

export function getBootstrapContext(state: BootstrapState): ApiContext {
  return {
    tenantId: state.tenant || null,
    key: state.key || state.public_key || '',
    urls: state.urls || {},
    webhookSecret: state.webhook_secret,
  };
}
