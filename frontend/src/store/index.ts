import { create } from 'zustand';

/**
 * The wire contract with viz-gateway. Mirrors ProjectionEvent in
 * services/viz-gateway/internal/domain/events.go field-for-field — change both or neither.
 */
export type Stage =
  | 'PO_CREATED'
  | 'VENDOR_CONFIRMED'
  | 'APPROVED'
  | 'IN_TRANSIT'
  | 'RECEIVED'
  // The gateway may introduce a stage before this file learns its name; render it, don't drop it.
  | (string & {});

/** Status vocabulary from services/viz-gateway/internal/domain/projection.go. */
export type Status = 'SUCCESS' | 'PENDING' | 'IN_PROGRESS' | 'FAILURE' | (string & {});

export interface P2PEvent {
  aggregate_id: string;
  stage: Stage;
  status: Status;
  /** HLC key as a decimal STRING. Never parse to number — it exceeds float64. */
  sequence_engine_key: string;
  occurred_at: string;
  trace_parent?: string;
  metrics?: {
    value?: number;
    sla_breached?: boolean;
  };
}

/** Payload of a `watermark` SSE frame (api.Watermark in the gateway). */
export interface WatermarkEvent {
  resolved_ts: string;
}

/**
 * The ledger is bounded. The store grew without limit before, and every insert re-sorted the whole
 * array with BigInt arithmetic; on a busy stream the page slowed down in proportion to how long it
 * had been open. Oldest SETTLED rows are dropped first — they are the ones that can be recovered
 * exactly from replay, whereas an in-flight row exists nowhere else on the client.
 */
export const MAX_EVENTS = 10_000;

export function isFailure(e: P2PEvent): boolean {
  return e.status === 'FAILURE' || e.metrics?.sla_breached === true;
}

export function keyOf(e: P2PEvent): string {
  return `${e.aggregate_id}-${e.sequence_engine_key}`;
}

/** Compare two HLC keys exactly. */
export function compareSeq(a: string, b: string): number {
  const d = BigInt(a) - BigInt(b);
  return d > 0n ? 1 : d < 0n ? -1 : 0;
}

export function isValidEvent(x: unknown): x is P2PEvent {
  if (typeof x !== 'object' || x === null) return false;
  const e = x as Record<string, unknown>;
  if (typeof e.aggregate_id !== 'string' || typeof e.sequence_engine_key !== 'string') return false;
  if (typeof e.stage !== 'string' || typeof e.status !== 'string') return false;
  try {
    BigInt(e.sequence_engine_key);
  } catch {
    return false;
  }
  return true;
}

interface AppState {
  /** Ascending by sequence_engine_key, deduplicated on (aggregate_id, sequence_engine_key). */
  events: P2PEvent[];
  /** The settlement cursor. Monotonic: a later, lower watermark is ignored. */
  watermark: bigint;
  addEvents: (newEvents: P2PEvent[]) => void;
  setWatermark: (wm: bigint) => void;
  clear: () => void;
}

/**
 * Merge new events into the sorted, deduplicated ledger.
 *
 * Exported so the invariant is testable without React: nothing about it depends on the store.
 * Both inputs may be unsorted; the result is ascending and bounded to MAX_EVENTS.
 */
export function mergeEvents(existing: P2PEvent[], incoming: P2PEvent[], watermark: bigint): P2PEvent[] {
  if (incoming.length === 0) return existing;
  const seen = new Set(existing.map(keyOf));
  const fresh: P2PEvent[] = [];
  for (const e of incoming) {
    if (!isValidEvent(e)) continue;
    const k = keyOf(e);
    if (seen.has(k)) continue;
    seen.add(k);
    fresh.push(e);
  }
  if (fresh.length === 0) return existing;

  // Sort only what arrived, then merge two sorted runs: O(n + m log m) instead of re-sorting the
  // whole ledger with BigInt subtraction on every insert.
  fresh.sort((a, b) => compareSeq(a.sequence_engine_key, b.sequence_engine_key));
  const merged: P2PEvent[] = new Array(existing.length + fresh.length);
  let i = 0;
  let j = 0;
  let k = 0;
  while (i < existing.length && j < fresh.length) {
    merged[k++] =
      compareSeq(existing[i].sequence_engine_key, fresh[j].sequence_engine_key) <= 0
        ? existing[i++]
        : fresh[j++];
  }
  while (i < existing.length) merged[k++] = existing[i++];
  while (j < fresh.length) merged[k++] = fresh[j++];

  if (merged.length <= MAX_EVENTS) return merged;
  // Trim from the oldest end, but never below the watermark's in-flight rows: drop settled first.
  const excess = merged.length - MAX_EVENTS;
  let settledPrefix = 0;
  while (settledPrefix < merged.length && BigInt(merged[settledPrefix].sequence_engine_key) <= watermark) {
    settledPrefix++;
  }
  return merged.slice(Math.min(excess, settledPrefix));
}

export const useStore = create<AppState>((set) => ({
  events: [],
  watermark: 0n,
  addEvents: (newEvents) =>
    set((state) => {
      const events = mergeEvents(state.events, newEvents, state.watermark);
      return events === state.events ? state : { events };
    }),
  setWatermark: (wm) =>
    set((state) => (wm > state.watermark ? { watermark: wm } : state)),
  clear: () => set({ events: [], watermark: 0n }),
}));
