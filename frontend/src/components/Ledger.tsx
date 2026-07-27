'use client';

import { useStore, type P2PEvent } from '@/store';

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

  if (events.length === 0) return <EmptyLedger />;

  // events arrive sorted ascending by HLC; newest-first reads better in a live record.
  const ordered = [...events].reverse();
  const inFlight = ordered.filter((e) => BigInt(e.sequence_engine_key) > watermark);
  const settled = ordered.filter((e) => BigInt(e.sequence_engine_key) <= watermark);

  return (
    <section aria-label="Order ledger" className="min-w-0">
      <ColumnHeadings />

      {inFlight.length > 0 && (
        <div aria-label="In flight">
          {inFlight.map((e) => (
            <Row key={rowKey(e)} event={e} settled={false} />
          ))}
        </div>
      )}

      <SettledLine watermark={watermark} />

      {settled.length > 0 ? (
        <div aria-label="Settled">
          {settled.map((e) => (
            <Row key={rowKey(e)} event={e} settled />
          ))}
        </div>
      ) : (
        <p className="py-6 text-sm text-[var(--muted)]">
          Nothing has settled yet. Records appear here once the changefeed resolves past them.
        </p>
      )}
    </section>
  );
}

const rowKey = (e: P2PEvent) => `${e.aggregate_id}-${e.sequence_engine_key}`;

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
        {watermark === BigInt(0) ? '—' : watermark.toString()}
      </span>
      <span className="h-px flex-1 bg-[var(--settled)]" />
    </div>
  );
}

function Row({ event, settled }: { event: P2PEvent; settled: boolean }) {
  const breached = event.metrics?.sla_breached || event.status === 'FAILURE';
  const value = event.metrics?.value;

  return (
    <article
      className={[
        'grid grid-cols-[1fr_auto] items-baseline gap-4 border-b border-[var(--rule)] py-3',
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
        workflow. To produce one now, seed a single event:
      </p>
      <p className="tnum mt-4 inline-block bg-[var(--paper-sunk)] px-3 py-2 text-[13px]">
        go run ./tools/seed
      </p>
    </section>
  );
}

/** Stage names as a buyer would say them, not as the event bus spells them. */
function humanStage(stage: string): string {
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
