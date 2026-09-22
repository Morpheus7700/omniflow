'use client';

/** Root-layout boundary: the last resort when the layout itself fails. It must render its own html. */
export default function GlobalError({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: 'system-ui, sans-serif', padding: '3rem', maxWidth: 640 }}>
        <h1 style={{ fontSize: '1.5rem' }}>OmniFlow could not load.</h1>
        <p>The application shell failed before the ledger could render. Reloading usually resolves it.</p>
        <button
          type="button"
          onClick={reset}
          style={{ marginTop: '1.5rem', minHeight: 44, padding: '0 1rem' }}
        >
          Reload
        </button>
      </body>
    </html>
  );
}
