import { useEffect, useState, useRef } from 'react';
import { useStore, isValidEvent, type P2PEvent, type WatermarkEvent } from '@/store';
import { STREAM_URL, replayURL, MAX_REPLAY_LIMIT } from '@/lib/config';

/** Connection states the rail can render. */
export type StreamStatus =
  | 'connecting'
  | 'connected'
  /** The browser reconnected after a drop; the gateway replayed what was missed (Last-Event-ID). */
  | 'reconnected'
  | 'error'
  | 'replay_finished'
  /** A replay window failed: no stream is involved, so "disconnected" would be the wrong sentence. */
  | 'replay_failed';

/**
 * Fetch the replay endpoint and decode it, failing loudly and legibly.
 *
 * `res.ok` is checked BEFORE `.json()`. The gateway answers a non-200 with a JSON error body
 * ({"error":{"code","message"}}); parsing it as the success shape would turn "gateway returned
 * 503" into an opaque type error. The signal is threaded through so the caller's cleanup can
 * cancel an in-flight request: a replay still in the air when the user hits "Return to live"
 * would otherwise land AFTER the next effect has `clear()`ed the store and write the stale window
 * back in.
 */
async function fetchWindow(url: string, signal: AbortSignal): Promise<P2PEvent[]> {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const body = (await res.json()) as { error?: { code?: string; message?: string } };
      if (body?.error?.message) detail = `${body.error.code ?? res.status}: ${body.error.message}`;
    } catch {
      // not JSON — the status line is all we have
    }
    throw new Error(`viz-gateway rejected ${url}: ${detail}`);
  }
  const rows: unknown = await res.json();
  if (!Array.isArray(rows)) throw new Error(`viz-gateway returned a non-array replay for ${url}`);
  return rows.filter(isValidEvent);
}

/** An abort is this hook cancelling its own work during cleanup — expected, never an error. */
function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === 'AbortError';
}

/**
 * A fetch rejection when the gateway is simply not running is an expected state in local
 * development. Reporting it as `console.error` promotes it into the Next dev error overlay, which
 * reads as a crash. It is not: the UI already surfaces it as a disconnected stream.
 */
function report(context: string, err: unknown) {
  if (isAbort(err)) return;
  if (err instanceof TypeError) {
    console.warn(
      `${context}: cannot reach viz-gateway. Is the stack up? ` +
        `(expected at ${new URL(STREAM_URL).origin}; compose maps it to :8081)`,
    );
    return;
  }
  console.error(`${context}:`, err);
}

/** Parse a frame's JSON without letting a malformed frame throw out of the event listener. */
function parseFrame<T>(raw: string, guard: (x: unknown) => x is T): T | null {
  try {
    const v: unknown = JSON.parse(raw);
    return guard(v) ? v : null;
  } catch {
    return null;
  }
}

function isWatermark(x: unknown): x is WatermarkEvent {
  return typeof x === 'object' && x !== null && typeof (x as WatermarkEvent).resolved_ts === 'string';
}

/** The integer part of an HLC resolved timestamp ("1790023750749228000.0000000000"). */
function watermarkKey(resolvedTs: string): bigint | null {
  const [intPart] = resolvedTs.split('.');
  try {
    return BigInt(intPart);
  } catch {
    return null;
  }
}

export function useEventBuffer(replayFrom: bigint | null = null, replayTo: bigint | null = null) {
  const addEvents = useStore((state) => state.addEvents);
  const setWatermark = useStore((state) => state.setWatermark);
  const clear = useStore((state) => state.clear);
  const bufferRef = useRef<P2PEvent[]>([]);

  // Identity of the subscription this hook currently describes. Changing either bound tears the
  // old stream down and opens a new one, so the reported status has to fall back to 'connecting'
  // at that same moment.
  const subscription = `${replayFrom ?? 'live'}:${replayTo ?? 'live'}`;
  const [status, setStatus] = useState<StreamStatus>('connecting');
  const [subscribed, setSubscribed] = useState(subscription);

  // React's documented "adjust state when a prop changes" pattern. Resetting the status inside the
  // effect would schedule a second render pass, and the frame in between paints the PREVIOUS
  // stream's status against the new window.
  if (subscribed !== subscription) {
    setSubscribed(subscription);
    setStatus('connecting');
  }

  useEffect(() => {
    clear();

    let sseSource: EventSource | null = null;
    let isSnapshotLoaded = false;
    let snapshotCursor = 0n;
    let opens = 0;
    // One controller per effect run. Cleanup aborts it, so every fetch below is bounded by the
    // lifetime of the effect that started it and cannot write into a later run's store.
    const controller = new AbortController();

    const initStream = async (startCursor: bigint) => {
      sseSource = new EventSource(STREAM_URL);

      sseSource.addEventListener('movement', (e: MessageEvent) => {
        const data = parseFrame(e.data, isValidEvent);
        if (!data) return; // a malformed frame is dropped, not thrown into React's render
        if (!isSnapshotLoaded) {
          bufferRef.current.push(data);
        } else if (BigInt(data.sequence_engine_key) > snapshotCursor) {
          addEvents([data]);
        }
      });

      sseSource.addEventListener('watermark', (e: MessageEvent) => {
        const data = parseFrame(e.data, isWatermark);
        const key = data ? watermarkKey(data.resolved_ts) : null;
        if (key !== null) setWatermark(key); // the store keeps it monotonic
      });

      sseSource.onopen = async () => {
        opens += 1;
        if (opens > 1) {
          // The browser reconnected on its own and sent Last-Event-ID; the gateway replayed the
          // gap from the replay repository, and the store dedups any overlap. Nothing to refetch —
          // but the reader should know a gap was crossed, not just that the dot went green again.
          setStatus('reconnected');
          return;
        }
        setStatus('connected');
        // Subscribe-then-snapshot: the stream is open and buffering, so the snapshot's upper edge
        // is the exact point after which buffered live events apply. No gap, no duplicate.
        try {
          const history = await fetchWindow(
            replayURL(startCursor, 0, MAX_REPLAY_LIMIT),
            controller.signal,
          );
          if (history.length > 0) {
            snapshotCursor = BigInt(history[history.length - 1].sequence_engine_key);
            addEvents(history);
          } else {
            snapshotCursor = startCursor;
          }
          const validBuffer = bufferRef.current.filter(
            (ev) => BigInt(ev.sequence_engine_key) > snapshotCursor,
          );
          if (validBuffer.length > 0) addEvents(validBuffer);
          bufferRef.current = [];
          isSnapshotLoaded = true;
        } catch (err) {
          report('Failed to fetch snapshot', err);
          if (!isAbort(err)) setStatus('error');
        }
      };

      sseSource.onerror = () => {
        setStatus('error');
      };
    };

    if (replayFrom !== null && replayTo !== null) {
      // Replay mode: fetch history by range. Do not open SSE unless they rejoin live.
      fetchWindow(replayURL(replayFrom, replayTo, MAX_REPLAY_LIMIT), controller.signal)
        .then((history) => {
          if (history.length > 0) {
            addEvents(history);
            // A replayed window is settled by definition: everything in it has been resolved.
            setWatermark(BigInt(history[history.length - 1].sequence_engine_key));
          }
          setStatus('replay_finished');
        })
        .catch((err) => {
          report('Failed to fetch replay history', err);
          if (!isAbort(err)) setStatus('replay_failed');
        });
    } else {
      initStream(0n).catch((err) => {
        report('Failed to open stream', err);
        if (!isAbort(err)) setStatus('error');
      });
    }

    return () => {
      controller.abort();
      if (sseSource) sseSource.close();
      bufferRef.current = [];
    };
  }, [replayFrom, replayTo, addEvents, clear, setWatermark]);

  return { status };
}
