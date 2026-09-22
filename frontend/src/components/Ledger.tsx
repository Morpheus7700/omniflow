'use client';

import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { useStore, isFailure, keyOf, type P2PEvent } from '@/store';

/**
 * The ledger, and the signature element of this interface: the settled line.
 *
 * Records above the line have an HLC sequence key at or below the watermark, which means the
 * changefeed has resolved past them and their position in the global order can no longer change.
 * They are set in full ink. Records below it are still in flight — shown, but visibly provisional.
 *
 * Most streaming dashboards simply hide unresolved events. Showing both halves and drawing the
 * boundary is the point: an evaluator can see exactly where the system's guarantee currently ends,
 * which is the thing being sold.
 */
export function Ledger() {
  const events = useStore((s) => s.events);
  const watermark = useStore((s) => s.watermark);

  // events arrive sorted ascending by HLC; newest-first reads better in a live record. The split
  // is a single pass, memoised on the two inputs — it used to run three filters with a BigInt
  // parse per row on every render.
  const { inFlight, settled } = useMemo(() => {
    const inFlight: P2PEvent[] = [];
    const settled: P2PEvent[] = [];
    for (let i = events.length - 1; i >= 0; i--) {
      const e = events[i];
      (BigInt(e.sequence_engine_key) > watermark ? inFlight : settled).push(e);
    }
    return { inFlight, settled };
  }, [events, watermark]);

  if (events.length === 0) return <EmptyLedger />;

  return (
    <section aria-label="Order ledger" className="min-w-0">
      <ColumnHeadings />

      {/*
        Live regions: a screen reader is told that rows arrived and that the line moved, without
        being read the whole ledger. The counts change; the rows themselves are not announced.
      */}
      <p className="sr-only" aria-live="polite" aria-atomic="true">
        {inFlight.length} in flight, {settled.length} settled
        {watermark === 0n ? '' : `, settled through ${watermark.toString()}`}
      </p>

      {inFlight.length > 0 && (
        <div aria-label="In flight">
          {inFlight.map((e) => (
            <Row key={keyOf(e)} event={e} settled={false} />
          ))}
        </div>
      )}

      <SettledLine watermark={watermark} />

      {settled.length > 0 ? (
        <WindowedRows rows={settled} />
      ) : (
        <p className="py-6 text-sm text-[var(--muted)]">
          Nothing has settled yet. Records appear here once the changefeed resolves past them.
        </p>
      )}
    </section>
  );
}

/** Every row is this tall; windowing depends on it, so it is a constant, not a measurement. */
export const ROW_HEIGHT = 44;
const WINDOW_VIEWPORT = 560; // px; ~12 rows visible, the rest virtual
const OVERSCAN = 6;

/**
 * Windowed list: only the rows in (and just around) the viewport exist in the DOM.
 *
 * The settled half of the ledger grows without bound over a session (bounded only by the store's
 * MAX_EVENTS), and a replay can return 5000 rows at once. Rendering every row as an <article>
 * made the page's cost proportional to history rather than to what is on screen. Hand-rolled
 * because the frontend rules allow no new UI library; fixed row height keeps the arithmetic exact.
 */
export function WindowedRows({ rows }: { rows: P2PEvent[] }) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [height, setHeight] = useState(WINDOW_VIEWPORT);

  useEffect(() => {
    const el = viewportRef.current;
    if (!el) return;
    const onScroll = () => setScrollTop(el.scrollTop);
    el.addEventListener('scroll', onScroll, { passive: true });
    // Match the viewport to the space available, capped so the page keeps a footer in reach.
    const ro = new ResizeObserver(() => setHeight(Math.min(WINDOW_VIEWPORT, el.clientHeight || WINDOW_VIEWPORT)));
    ro.observe(el);
    return () => {
      el.removeEventListener('scroll', onScroll);
      ro.disconnect();
    };
  }, []);

  const total = rows.length;
  const first = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const last = Math.min(total, Math.ceil((scrollTop + height) / ROW_HEIGHT) + OVERSCAN);
  const visible = rows.slice(first, last);

  const viewportStyle: CSSProperties = { maxHeight: WINDOW_VIEWPORT, overflowY: 'auto' };
  const spacerStyle: CSSProperties = { height: total * ROW_HEIGHT, position: 'relative' };

  return (
    <div
      ref={viewportRef}
      aria-label="Settled"
      role="list"
      style={viewportStyle}
      data-testid="settled-window"
    >
      <div style={spacerStyle}>
        {visible.map((e, i) => (
          <div
            key={keyOf(e)}
            role="listitem"
            aria-setsize={total}
            aria-posinset={first + i + 1}
            style={{ position: 'absolute', top: (first + i) * ROW_HEIGHT, left: 0, right: 0, height: ROW_HEIGHT }}
          >
            <Row event={e} settled />
          </div>
        ))}
      </div>
    </div>
  );
}

function ColumnHeadings() {
  return (
    <div className="grid grid-cols-[1fr_auto] items-baseline gap-4 border-b border-[var(--rule)] pb-2 text-[11px] uppercase tracking-[0.14em] text-[var(--muted)] sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)_auto_auto]">
      <span>Order</span>
      <span className="hidden sm:block">Stage</span>
      <span className="hidden text-right sm:block">Value</span>
      <span className="text-right">Sequence</span>
    </div>
  );
}

/**
 * The boundary itself. It is labelled, because an unlabelled rule is decoration — the number is what
 * makes it a claim rather than a flourish.
 */
function SettledLine({ watermark }: { watermark: bigint }) {
  return (
    <div className="relative my-1 flex items-center gap-3 py-3" role="separator" aria-label="Settlement boundary">
      <span className="whitespace-nowrap text-[11px] font-medium uppercase tracking-[0.14em] text-[var(--settled)]">
        Settled through
      </span>
      <span className="tnum whitespace-nowrap text-[11px] text-[var(--settled)]">
        {watermark === 0n ? '—' : watermark.toString()}
      </span>
      <span className="h-px flex-1 bg-[var(--settled)]" />
    </div>
  );
}

function Row({ event, settled }: { event: P2PEvent; settled: boolean }) {
  const breached = isFailure(event);
  const pending = event.status === 'PENDING';
  const value = event.metrics?.value;

  return (
    <article
      style={{ height: ROW_HEIGHT }}
      className={[
        'grid grid-cols-[1fr_auto] items-center gap-4 border-b border-[var(--rule)]',
        'sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)_auto_auto]',
        settled ? 'text-[var(--ink)]' : 'text-[var(--muted)]',
      ].join(' ')}
    >
      <span className="tnum min-w-0 truncate text-[13px]" title={event.aggregate_id}>
        {event.aggregate_id}
      </span>

      <span className="hidden min-w-0 truncate text-[13px] sm:block">
        {breached ? (
          <span className="text-[var(--exception)]">{humanStage(event.stage)} · exception</span>
        ) : pending ? (
          <span>{humanStage(event.stage)} · awaiting approval</span>
        ) : (
          humanStage(event.stage)
        )}
      </span>

      <span
        className={`tnum hidden text-right text-[13px] sm:block ${
          breached ? 'text-[var(--exception)]' : ''
        }`}
      >
        {typeof value === 'number' ? formatValue(value) : '—'}
      </span>

      <span className="tnum text-right text-[12px] text-[var(--muted)]">
        {event.sequence_engine_key}
      </span>
    </article>
  );
}

function EmptyLedger() {
  return (
    <section className="border-t border-[var(--rule)] py-16" aria-label="Order ledger">
      <p className="font-serif text-2xl">No orders on the wire yet.</p>
      <p className="mt-3 max-w-prose text-sm leading-relaxed text-[var(--muted)]">
        Orders appear the moment CommBot classifies a vendor message and the orchestrator opens a
        workflow. The ledger fills itself; there is nothing to do here.
      </p>
    </section>
  );
}

/** Stage names as a buyer would say them, not as the event bus spells them. */
export function humanStage(stage: string): string {
  const named: Record<string, string> = {
    PO_CREATED: 'Order raised',
    VENDOR_CONFIRMED: 'Vendor confirmed',
    APPROVED: 'Approved',
    IN_TRANSIT: 'In transit',
    RECEIVED: 'Received',
  };
  return named[stage] ?? stage.toLowerCase().replace(/_/g, ' ');
}

function formatValue(v: number): string {
  return v.toLocaleString(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  });
}
