import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { BootstrapState, readBootstrapState, resolveAccessKey, resolveTenantId, resolveUrls } from '../lib/bootstrap';
import { ApiContext, buildUrl, requestJson } from '../lib/api';

export type SettingsPayload = {
  ok?: boolean;
  cfg?: Record<string, unknown>;
  persona?: string;
  personas?: Record<string, string>;
};

export type ClientContextValue = {
  bootstrap: BootstrapState;
  api: ApiContext;
  settings: SettingsPayload | null;
  refreshSettings: () => Promise<void>;
  setSettings: React.Dispatch<React.SetStateAction<SettingsPayload | null>>;
};

const ClientContext = createContext<ClientContextValue | null>(null);

export const ClientProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const bootstrap = useMemo(() => {
    const state = readBootstrapState();
    return {
      ...state,
      tenant: resolveTenantId(state) || state.tenant,
      key: resolveAccessKey(state) || state.key,
      urls: resolveUrls(state),
    };
  }, []);

  const api = useMemo<ApiContext>(() => {
    return {
      tenantId: bootstrap.tenant || null,
      key: bootstrap.key || bootstrap.public_key || '',
      urls: bootstrap.urls || {},
      webhookSecret: bootstrap.webhook_secret,
    };
  }, [bootstrap]);

  const [settings, setSettings] = useState<SettingsPayload | null>(null);

  const refreshSettings = async () => {
    const settingsUrl = bootstrap.urls?.settings_get || '/pub/settings/get';
    if (!api.tenantId || !api.key) return;
    const target = buildUrl(settingsUrl, api, { _: Date.now() });
    const data = await requestJson<SettingsPayload>(target);
    setSettings(data);
  };

  useEffect(() => {
    refreshSettings().catch(() => undefined);
  }, [api.tenantId, api.key]);

  return (
    <ClientContext.Provider value={{ bootstrap, api, settings, refreshSettings, setSettings }}>
      {children}
    </ClientContext.Provider>
  );
};

export function useClient() {
  const ctx = useContext(ClientContext);
  if (!ctx) {
    throw new Error('ClientContext missing');
  }
  return ctx;
}
