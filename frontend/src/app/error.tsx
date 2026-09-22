'use client';

/**
 * Route error boundary. Before this existed a thrown render error — a malformed event reaching a
 * BigInt() call, say — white-screened the page with no way back short of a reload.
 */
export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <main className="mx-auto flex min-h-screen max-w-[720px] flex-col justify-center px-6">
      <p className="text-[11px] uppercase tracking-[0.14em] text-[var(--exception)]">
        The ledger could not render
      </p>
      <h1 className="mt-3 font-serif text-3xl">Something in the record could not be drawn.</h1>
      <p className="mt-4 max-w-prose text-sm leading-relaxed text-[var(--muted)]">
        The event stream and the gateway are unaffected; this is the dashboard failing to display
        what it received. Retrying re-opens the stream and rebuilds the ledger from the replay
        endpoint.
      </p>
      {error.digest && (
        <p className="tnum mt-3 text-[12px] text-[var(--muted)]">reference {error.digest}</p>
      )}
      <button
        type="button"
        onClick={reset}
        className="mt-8 min-h-11 self-start border border-[var(--ink)] bg-[var(--ink)] px-4 text-[13px] text-[var(--paper)]"
      >
        Try again
      </button>
    </main>
  );
}
