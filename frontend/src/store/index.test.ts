import { describe, it, expect, beforeEach } from 'vitest';
import { mergeEvents, useStore, MAX_EVENTS, isValidEvent, type P2PEvent } from './index';

const ev = (key: string, agg = 'a', extra: Partial<P2PEvent> = {}): P2PEvent => ({
  aggregate_id: agg,
  stage: 'PO_CREATED',
  status: 'SUCCESS',
  sequence_engine_key: key,
  occurred_at: '2026-09-22T00:00:00Z',
  ...extra,
});

describe('mergeEvents', () => {
  it('keeps the ledger sorted by exact HLC key, not by string or float order', () => {
    // 1790023750749228000 vs 1790023750749228001 differ only in the last digit: float64 would
    // collapse them; string order would put "9" after "18...". BigInt keeps both exact.
    const a = ev('1790023750749228001');
    const b = ev('1790023750749228000');
    const c = ev('9');
    const out = mergeEvents([], [a, b, c], 0n);
    expect(out.map((e) => e.sequence_engine_key)).toEqual([
      '9',
      '1790023750749228000',
      '1790023750749228001',
    ]);
  });

  it('deduplicates on (aggregate_id, sequence_engine_key) and returns the same array when nothing is new', () => {
    const base = mergeEvents([], [ev('1'), ev('2')], 0n);
    const again = mergeEvents(base, [ev('1'), ev('2', 'a')], 0n);
    expect(again).toBe(base); // identity preserved → no re-render
    const other = mergeEvents(base, [ev('2', 'b')], 0n);
    expect(other).toHaveLength(3);
  });

  it('merges an unsorted late arrival into the right position', () => {
    const base = mergeEvents([], [ev('10'), ev('30')], 0n);
    const out = mergeEvents(base, [ev('20')], 0n);
    expect(out.map((e) => e.sequence_engine_key)).toEqual(['10', '20', '30']);
  });

  it('drops malformed events instead of letting them reach a BigInt() in render', () => {
    const bad = { aggregate_id: 'x', stage: 'PO_CREATED', status: 'SUCCESS', sequence_engine_key: 'not-a-key' };
    const out = mergeEvents([], [bad as unknown as P2PEvent, ev('1')], 0n);
    expect(out).toHaveLength(1);
    expect(isValidEvent(bad)).toBe(false);
  });

  it('bounds the ledger, dropping oldest SETTLED rows first and never in-flight ones', () => {
    const many: P2PEvent[] = [];
    for (let i = 1; i <= MAX_EVENTS + 50; i++) many.push(ev(String(i), `agg-${i}`));
    // Watermark covers only the first 20: only those may be dropped.
    const out = mergeEvents([], many, 20n);
    expect(out.length).toBe(MAX_EVENTS + 50 - 20);
    expect(out[0].sequence_engine_key).toBe('21');
    // With everything settled, the excess is dropped from the oldest end.
    const out2 = mergeEvents([], many, 10n ** 12n);
    expect(out2.length).toBe(MAX_EVENTS);
    expect(out2[0].sequence_engine_key).toBe('51');
  });
});

describe('useStore', () => {
  beforeEach(() => useStore.getState().clear());

  it('never lets the watermark move backwards', () => {
    useStore.getState().setWatermark(100n);
    useStore.getState().setWatermark(50n);
    expect(useStore.getState().watermark).toBe(100n);
    useStore.getState().setWatermark(150n);
    expect(useStore.getState().watermark).toBe(150n);
  });

  it('clear resets both the ledger and the cursor', () => {
    useStore.getState().addEvents([ev('1')]);
    useStore.getState().setWatermark(1n);
    useStore.getState().clear();
    expect(useStore.getState().events).toEqual([]);
    expect(useStore.getState().watermark).toBe(0n);
  });
});
