import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

// RTL only auto-cleans when vitest globals are on; they are off here, so unmount explicitly or
// every test's render accumulates in one document.
afterEach(() => cleanup());
