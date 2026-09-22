import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useEventBuffer } from './useEventBuffer';
import { useStore, type P2PEvent } from '@/store';

/**
 * A scriptable EventSource. The real one is the browser's; what matters here is the ORDER of
 * open / movement / watermark relative to the snapshot fetch, which is exactly what the hook's
 * no-gap/no-dup handover depends on.
 */
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;
  url: string;
  readyState = 0;
  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  private listeners = new Map<string, Array<(e: MessageEvent) => void>>();
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (e: MessageEvent) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn]);
  }
  removeEventListener() {}
  close() {
    this.closed = true;
    this.readyState = 2;
  }
  // test controls
  open() {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }
  emit(type: string, data: unknown, id?: string) {
    for (const fn of this.listeners.get(type) ?? []) {
      fn(new MessageEvent(type, { data: JSON.stringify(data), lastEventId: id ?? '' }));
    }
  }
  fail() {
    this.onerror?.(new Event('error'));
  }
}

const ev = (key: string, agg = `agg-${key}`): P2PEvent => ({
  aggregate_id: agg,
  stage: 'PO_CREATED',
  status: 'SUCCESS',
  sequence_engine_key: key,
  occurred_at: '2026-09-22T00:00:00Z',
});

type Deferred<T> = { promise: Promise<T>; resolve: (v: T) => void; reject: (e: unknown) => void };
function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

describe('useEventBuffer', () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    useStore.getState().clear();
    FakeEventSource.instances = [];
    vi.stubGlobal('EventSource', FakeEventSource);
    fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('buffers live events until the snapshot lands, then applies only those past the snapshot cursor — no gap, no duplicate', async () => {
    const snapshot = deferred<Response>();
    fetchMock.mockReturnValueOnce(snapshot.promise);

    const { result } = renderHook(() => useEventBuffer());
    const es = FakeEventSource.instances[0];
    expect(es).toBeDefined();

    // Stream opens; the snapshot request starts and is still in flight.
    act(() => es.open());
    expect(result.current.status).toBe('connected');
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toMatch(/from_seq=0&to_seq=0&limit=5000/);

    // Live events arrive DURING the snapshot: one that the snapshot will also contain (dup) and
    // one newer than the snapshot's edge (must be kept).
    act(() => {
      es.emit('movement', ev('100'), '100');
      es.emit('movement', ev('101'), '101');
    });
    expect(useStore.getState().events).toHaveLength(0); // buffered, not applied

    // Snapshot resolves with history up to key 100.
    await act(async () => {
      snapshot.resolve(jsonResponse([ev('98'), ev('99'), ev('100')]));
      await snapshot.promise;
    });

    await waitFor(() => expect(useStore.getState().events.map((e) => e.sequence_engine_key)).toEqual(['98', '99', '100', '101']));

    // After the handover, live events apply directly.
    act(() => es.emit('movement', ev('102'), '102'));
    expect(useStore.getState().events.map((e) => e.sequence_engine_key)).toEqual(['98', '99', '100', '101', '102']);
  });

  it('applies watermarks monotonically and ignores malformed frames', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse([]));
    renderHook(() => useEventBuffer());
    const es = FakeEventSource.instances[0];
    await act(async () => es.open());

    act(() => {
      es.emit('watermark', { resolved_ts: '1790023750749228000.0000000000' });
      es.emit('watermark', { resolved_ts: '1790023750749227000.0000000000' }); // older: ignored
      es.emit('watermark', { nonsense: true });
    });
    expect(useStore.getState().watermark).toBe(1790023750749228000n);

    // A frame that is not JSON must not throw out of the listener.
    expect(() => {
      for (const fn of (es as unknown as { listeners: Map<string, Array<(e: MessageEvent) => void>> }).listeners.get('movement') ?? []) {
        fn(new MessageEvent('movement', { data: '{not json' }));
      }
    }).not.toThrow();
  });

  it('reports a reconnect distinctly from the first connection and does not re-snapshot', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse([ev('1')]));
    const { result } = renderHook(() => useEventBuffer());
    const es = FakeEventSource.instances[0];
    await act(async () => es.open());
    await waitFor(() => expect(useStore.getState().events).toHaveLength(1));

    act(() => es.fail());
    expect(result.current.status).toBe('error');

    // The browser reconnects the SAME EventSource (sending Last-Event-ID itself).
    await act(async () => es.open());
    expect(result.current.status).toBe('reconnected');
    expect(fetchMock).toHaveBeenCalledTimes(1); // the gateway replayed the gap; no second snapshot
  });

  it('surfaces a gateway JSON error body instead of a JSON parse error', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({ error: { code: 'rate_limited', message: 'too many replay requests' } }, 429),
    );
    const { result } = renderHook(() => useEventBuffer());
    await act(async () => FakeEventSource.instances[0].open());
    await waitFor(() => expect(result.current.status).toBe('error'));
    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to fetch snapshot'),
      expect.objectContaining({ message: expect.stringContaining('rate_limited: too many replay requests') }),
    );
  });

  it('reports a failed replay distinctly from a dropped stream', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({ error: { code: 'invalid_limit', message: 'limit out of range' } }, 400),
    );
    const { result } = renderHook(() => useEventBuffer(1n, 2n));
    await waitFor(() => expect(result.current.status).toBe('replay_failed'));
  });

  it('replay mode fetches the window, settles it, and opens no stream', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse([ev('5'), ev('6')]));
    const { result } = renderHook(() => useEventBuffer(5n, 6n));
    await waitFor(() => expect(result.current.status).toBe('replay_finished'));
    expect(FakeEventSource.instances).toHaveLength(0);
    expect(useStore.getState().watermark).toBe(6n);
    expect(String(fetchMock.mock.calls[0][0])).toMatch(/from_seq=5&to_seq=6&limit=5000/);
  });

  it('closes the stream and aborts the snapshot on unmount', async () => {
    const snapshot = deferred<Response>();
    fetchMock.mockReturnValueOnce(snapshot.promise);
    const { unmount } = renderHook(() => useEventBuffer());
    const es = FakeEventSource.instances[0];
    act(() => es.open());
    const signal = (fetchMock.mock.calls[0][1] as RequestInit).signal as AbortSignal;
    unmount();
    expect(es.closed).toBe(true);
    expect(signal.aborted).toBe(true);
  });
});
