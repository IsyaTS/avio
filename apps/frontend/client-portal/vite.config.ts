import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  base: '/static/spa/client/',
  build: {
    outDir: '../../api/static/spa/client',
    emptyOutDir: true,
    manifest: 'manifest.json',
    rollupOptions: {
      input: 'index.html',
    },
  },
});
