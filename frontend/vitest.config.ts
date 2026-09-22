import { defineConfig } from 'vitest/config';
import path from 'node:path';

// No @vitejs/plugin-react: Vite's default automatic JSX runtime is enough for these tests and
// keeps the toolchain to what is already installed.
export default defineConfig({
  resolve: { alias: { '@': path.resolve(__dirname, 'src') } },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}'],
    setupFiles: ['./vitest.setup.ts'],
    globals: false,
  },
});
