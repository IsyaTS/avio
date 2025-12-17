import React, { useEffect, useState } from 'react';
import { NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { ClientProvider } from './context/ClientContext';
import SettingsTab from './pages/SettingsTab';
import ChannelsTab from './pages/ChannelsTab';
import CatalogTab from './pages/CatalogTab';
import TrainingTab from './pages/TrainingTab';
import StatsTab from './pages/StatsTab';

const navItems = [
  { label: 'Настройки', path: '/settings' },
  { label: 'Каналы', path: '/channels' },
  { label: 'Каталог', path: '/catalog' },
  { label: 'Обучение', path: '/training' },
  { label: 'Статистика', path: '/stats' },
];

const AppLayout: React.FC = () => {
  const [drawerOpen, setDrawerOpen] = useState(false);

  useEffect(() => {
    const hash = window.location.hash || '';
    if (hash && !hash.startsWith('#/')) {
      const normalized = hash.replace(/^#/, '').replace(/^\//, '');
      window.location.hash = `#/${normalized}`;
    }
  }, []);

  return (
    <div className="min-h-screen bg-slate-50">
      <div className="flex">
        <aside className="hidden lg:flex lg:w-72 lg:flex-col lg:border-r lg:border-slate-100 lg:bg-white">
          <div className="px-6 py-6">
            <div className="text-sm uppercase tracking-[0.3em] text-brand-500">Avio</div>
            <div className="text-2xl font-semibold text-slate-900" style={{ fontFamily: 'Space Grotesk' }}>
              Клиентский кабинет
            </div>
          </div>
          <nav className="flex-1 px-4 pb-6">
            <div className="space-y-2">
              {navItems.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.path}
                  className={({ isActive }) =>
                    `sidebar-link ${isActive ? 'sidebar-link-active' : ''}`
                  }
                >
                  <span>{item.label}</span>
                </NavLink>
              ))}
            </div>
          </nav>
        </aside>

        {drawerOpen && (
          <div className="fixed inset-0 z-40 bg-slate-900/40 lg:hidden" onClick={() => setDrawerOpen(false)} />
        )}
        <aside
          className={`fixed left-0 top-0 z-50 h-full w-72 transform bg-white shadow-xl transition-transform lg:hidden ${
            drawerOpen ? 'translate-x-0' : '-translate-x-full'
          }`}
        >
          <div className="px-6 py-6 flex items-start justify-between">
            <div>
              <div className="text-sm uppercase tracking-[0.3em] text-brand-500">Avio</div>
              <div className="text-xl font-semibold text-slate-900" style={{ fontFamily: 'Space Grotesk' }}>
                Клиентский кабинет
              </div>
            </div>
            <button className="btn-ghost" onClick={() => setDrawerOpen(false)}>
              ✕
            </button>
          </div>
          <nav className="px-4 pb-6">
            <div className="space-y-2">
              {navItems.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.path}
                  onClick={() => setDrawerOpen(false)}
                  className={({ isActive }) =>
                    `sidebar-link ${isActive ? 'sidebar-link-active' : ''}`
                  }
                >
                  <span>{item.label}</span>
                </NavLink>
              ))}
            </div>
          </nav>
        </aside>

        <div className="flex min-h-screen flex-1 flex-col">
          <header className="sticky top-0 z-30 flex items-center justify-between border-b border-slate-100 bg-white/90 px-4 py-4 backdrop-blur lg:px-8">
            <button className="btn-ghost lg:hidden" onClick={() => setDrawerOpen(true)}>
              ☰
            </button>
            <div>
              <div className="text-sm text-slate-500">Настройки клиента</div>
              <div className="text-lg font-semibold text-slate-900" style={{ fontFamily: 'Space Grotesk' }}>
                Admin Dashboard
              </div>
            </div>
            <div className="hidden items-center gap-3 lg:flex">
              <span className="badge badge-neutral">beta</span>
            </div>
          </header>

          <main className="flex-1 px-4 py-6 lg:px-10">
            <Routes>
              <Route path="/" element={<Navigate to="/settings" replace />} />
              <Route path="/settings" element={<SettingsTab />} />
              <Route path="/channels/*" element={<ChannelsTab />} />
              <Route path="/catalog" element={<CatalogTab />} />
              <Route path="/training" element={<TrainingTab />} />
              <Route path="/dialogs" element={<Navigate to="/training" replace />} />
              <Route path="/stats" element={<StatsTab />} />
              <Route path="*" element={<Navigate to="/settings" replace />} />
            </Routes>
          </main>
        </div>
      </div>
    </div>
  );
};

const App: React.FC = () => (
  <ClientProvider>
    <AppLayout />
  </ClientProvider>
);

export default App;
