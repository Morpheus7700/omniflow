'use client';

// FormEvent is imported as a type rather than reached through the `React.` namespace: with
// jsx: "react-jsx" there is no React import in scope, and namespace access requires
// allowUmdGlobalAccess, which this project does not set.
import { useMemo, useState, type FormEvent } from 'react';
import { useStore, isFailure } from '@/store';
import type { StreamStatus } from '@/hooks/useEventBuffer';

/**
 * The left rail: what is true right now, and the controls to move through time.
 *
 * Figures are stacked rather than boxed in cards. Cards imply each number is an independent widget;
 * these are three readings off one ledger, so they share a column and a set of rules.
 */
export function Rail({
  status,
  replaying,
  onReplay,
  onGoLive,
}: {
  status: StreamStatus;
  replaying: boolean;
  onReplay: (from: bigint, to: bigint) => void;
  onGoLive: () => void;
}) {
  const events = useStore((s) => s.events);
  const watermark = useStore((s) => s.watermark);

  // One pass, memoised: three filters with a BigInt parse per row ran on every render before.
  const { settledCount, value, exceptions } = useMemo(() => {
    let settledCount = 0;
    let value = 0;
    let exceptions = 0;
    for (const e of events) {
      if (BigInt(e.sequence_engine_key) > watermark) continue;
      settledCount++;
      value += e.metrics?.value ?? 0;
      if (isFailure(e)) exceptions++;
    }
    return { settledCount, value, exceptions };
  }, [events, watermark]);

  return (
    <div className="flex h-full flex-col gap-8">
      <Connection status={status} />

      <dl className="flex flex-col">
        <Figure label="Settled orders" value={settledCount.toLocaleString()} />
        <Figure
          label="Settled value"
          value={value.toLocaleString(undefined, {
            style: 'currency',
            currency: 'USD',
            maximumFractionDigits: 0,
          })}
        />
        <Figure
          label="Exceptions"
          value={exceptions.toLocaleString()}
          tone={exceptions > 0 ? 'exception' : 'default'}
        />
      </dl>

      <Replay replaying={replaying} onReplay={onReplay} onGoLive={onGoLive} />
    </div>
  );
}

function Figure({
  label,
  value,
  tone = 'default',
}: {
  label: string;
  value: string;
  tone?: 'default' | 'exception';
}) {
  return (
    <div className="border-b border-[var(--rule)] py-4">
      <dt className="text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">{label}</dt>
      <dd
        className={`tnum mt-1 text-2xl ${
          tone === 'exception' ? 'text-[var(--exception)]' : 'text-[var(--ink)]'
        }`}
      >
        {value}
      </dd>
    </div>
  );
}

/**
 * Connection state is a fact about the record, not a decoration: if the stream is down the figures
 * above are stale, and the reader needs to know that without hunting for a toast.
 */
function Connection({ status }: { status: StreamStatus }) {
  const map: Record<StreamStatus, { label: string; tone: string; dot: string }> = {
    connecting: { label: 'Connecting to the event stream', tone: 'text-[var(--muted)]', dot: 'bg-[var(--muted)]' },
    connected: { label: 'Live', tone: 'text-[var(--settled)]', dot: 'bg-[var(--settled)]' },
    reconnected: {
      label: 'Live — reconnected; the gap was replayed from the gateway',
      tone: 'text-[var(--settled)]',
      dot: 'bg-[var(--settled)]',
    },
    error: { label: 'Stream disconnected — figures below are stale', tone: 'text-[var(--exception)]', dot: 'bg-[var(--exception)]' },
    replay_finished: { label: 'Showing a replayed window', tone: 'text-[var(--muted)]', dot: 'bg-[var(--muted)]' },
    replay_failed: {
      label: 'That window could not be replayed — the gateway refused or is unreachable',
      tone: 'text-[var(--exception)]',
      dot: 'bg-[var(--exception)]',
    },
  };
  const s = map[status];

  // role="status" is a polite live region: a reader is told the stream dropped without hunting
  // for a toast, and without the announcement interrupting them mid-sentence.
  return (
    <p role="status" aria-live="polite" className={`flex items-center gap-2 text-[13px] ${s.tone}`}>
      <span aria-hidden className={`inline-block h-1.5 w-1.5 rounded-full ${s.dot}`} />
      {s.label}
    </p>
  );
}

function Replay({
  replaying,
  onReplay,
  onGoLive,
}: {
  replaying: boolean;
  onReplay: (from: bigint, to: bigint) => void;
  onGoLive: () => void;
}) {
  const [from, setFrom] = useState('0');
  const [to, setTo] = useState('0');
  const [error, setError] = useState<string | null>(null);

  function submit(e: FormEvent) {
    e.preventDefault();
    try {
      const f = BigInt(from.trim() || '0');
      const t = BigInt(to.trim() || '0');
      if (t !== BigInt(0) && t < f) {
        setError('The end of the window must come after the start.');
        return;
      }
      setError(null);
      onReplay(f, t);
    } catch {
      setError('Sequence keys are whole numbers.');
    }
  }

  return (
    <form onSubmit={submit} className="mt-auto border-t border-[var(--rule)] pt-5">
      <h2 className="text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">Replay a window</h2>
      <p className="mt-2 text-[13px] leading-relaxed text-[var(--muted)]">
        Rebuild the ledger between two sequence keys, exactly as it happened.
      </p>

      <div className="mt-4 flex gap-2">
        <Field label="From" value={from} onChange={setFrom} />
        <Field label="To" value={to} onChange={setTo} />
      </div>

      {error && (
        <p role="alert" className="mt-2 text-[12px] text-[var(--exception)]">
          {error}
        </p>
      )}

      <div className="mt-4 flex gap-2">
        <button
          type="submit"
          className="min-h-11 border border-[var(--ink)] bg-[var(--ink)] px-4 text-[13px] text-[var(--paper)] transition-opacity hover:opacity-85"
        >
          Replay
        </button>
        {replaying && (
          <button
            type="button"
            onClick={onGoLive}
            className="min-h-11 border border-[var(--rule)] px-4 text-[13px] transition-colors hover:border-[var(--ink)]"
          >
            Return to live
          </button>
        )}
      </div>
    </form>
  );
}

function Field({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <label className="flex-1">
      <span className="block text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">
        {label}
      </span>
      <input
        value={value}
        // inputMode only (a keyboard hint). NOT `pattern`: a pattern makes the browser block
        // submit with its own generic bubble, so the form's specific messages — "sequence keys are
        // whole numbers", "the end of the window must come after the start" — never run.
        inputMode="numeric"
        onChange={(e) => onChange(e.target.value)}
        className="tnum mt-1 min-h-11 w-full border-b border-[var(--rule)] bg-transparent text-[13px] focus:border-[var(--settled)]"
      />
    </label>
  );
}
