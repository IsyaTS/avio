export type ClientUrls = Record<string, string | undefined> & {
  settings_get?: string;
  settings_save?: string;
  save_settings?: string;
  save_persona?: string;
  save_behavior?: string;
  save_followups?: string;
  get_followups?: string;
  upload_catalog?: string;
  csv_get?: string;
  csv_save?: string;
  training_upload?: string;
  training_status?: string;
  whatsapp_export?: string;
  dialogs_list?: string;
  dialogs_detail?: string;
  dialogs_send?: string;
  feedback?: string;
  feedback_stats?: string;
};

export type BootstrapBehavior = {
  auto_reply?: boolean;
  auto_reply_text?: string;
  avito_phone_tg_template?: string;
  avito_smart_reply_enabled?: boolean;
  telegram_reply_enabled?: boolean;
  send_catalog_on_first_message?: boolean;
  triggers?: Array<{
    phrases?: string[];
    channels?: string[];
    silence?: boolean;
    notify?: boolean;
  }>;
  photo_expected_markers?: string[];
  photo_expected_reply?: string;
  photo_expected_ttl?: number;
};

export type BootstrapForm = {
  brand?: string;
  agent?: string;
  city?: string;
  catalog_file?: string;
  currency?: string;
  tone?: string;
};

export type BootstrapState = {
  tenant?: number;
  key?: string;
  public_key?: string;
  primary_key?: string;
  webhook_secret?: string;
  urls?: ClientUrls;
  form?: BootstrapForm;
  behavior?: BootstrapBehavior;
};

declare global {
  interface Window {
    __client_settings_state?: BootstrapState;
  }
}

function parseJson(text: string | null): BootstrapState | null {
  if (!text) return null;
  try {
    return JSON.parse(text) as BootstrapState;
  } catch {
    return null;
  }
}

export function readBootstrapState(): BootstrapState {
  const fromWindow = window.__client_settings_state;
  if (fromWindow && typeof fromWindow === 'object') {
    return fromWindow;
  }
  const node = document.getElementById('client-settings-state');
  if (node) {
    const parsed = parseJson(node.textContent);
    if (parsed) return parsed;
  }
  return {};
}

export function resolveTenantId(state: BootstrapState): number | null {
  if (state.tenant && Number.isFinite(state.tenant)) {
    return state.tenant;
  }
  const match = window.location.pathname.match(/\/client\/(\d+)/);
  if (match && match[1]) {
    const parsed = Number.parseInt(match[1], 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

export function resolveAccessKey(state: BootstrapState): string {
  const params = new URLSearchParams(window.location.search);
  const fromQuery = (params.get('k') || '').trim();
  if (fromQuery) return fromQuery;
  if (state.key) return state.key;
  if (state.public_key) return state.public_key;
  const cookieMatch = document.cookie.match(/(?:^|;\s*)client_key=([^;]+)/);
  if (cookieMatch && cookieMatch[1]) {
    return decodeURIComponent(cookieMatch[1]);
  }
  return '';
}

export function resolveUrls(state: BootstrapState): ClientUrls {
  return state.urls || {};
}
