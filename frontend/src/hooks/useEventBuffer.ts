import { useEffect, useState, useRef } from 'react';
import { useStore, P2PEvent } from '@/store';
import { STREAM_URL, replayURL } from '@/lib/config';

/**
 * Fetch the replay endpoint and decode it, failing loudly and legibly.
 *
 * Two things here are deliberate, and both were bugs before:
 *
 * `res.ok` is checked BEFORE `.json()`. The gateway answers a non-200 with a text body, so parsing
 * it unconditionally turned "gateway returned 503" into an opaque `SyntaxError: Unexpected token`
 * pointing at the JSON parser — an error message that describes the symptom and hides the cause.
 *
 * The signal is threaded through so the caller's cleanup can cancel an in-flight request. Without
 * it a replay that is still in the air when the user hits "Return to live" lands AFTER the next
 * effect has already `clear()`ed the store, and writes the stale window back in — a race that
 * shows the wrong ledger and looks like a backend ordering fault rather than a client one.
 */
async function fetchWindow(url: string, signal: AbortSignal): Promise<P2PEvent[]> {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    throw new Error(`viz-gateway returned ${res.status} ${res.statusText} for ${url}`);
  }
  return (await res.json()) as P2PEvent[];
}

/** An abort is this hook cancelling its own work during cleanup — expected, never an error. */
function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === 'AbortError';
}

/**
 * A fetch rejection when the gateway is simply not running is an expected state in local
 * development — the frontend is presentation-only and the stack lives in Docker. Reporting it as
 * `console.error` promotes it into the Next dev error overlay, which reads as a crash in the app.
 * It is not: the UI already surfaces it as "Stream disconnected — figures below are stale".
 *
 * So a transport failure is a warning naming the origin and what to do about it, while anything
 * else — a real decode fault, an unexpected status — stays an error, because that IS a defect.
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

export function useEventBuffer(replayFrom: bigint | null = null, replayTo: bigint | null = null) {
  const addEvents = useStore(state => state.addEvents);
  const setWatermark = useStore(state => state.setWatermark);
  const clear = useStore(state => state.clear);
  const bufferRef = useRef<P2PEvent[]>([]);

  // Identity of the subscription this hook currently describes. Changing either bound tears the
  // old stream down and opens a new one, so the reported status has to fall back to 'connecting'
  // at that same moment.
  const subscription = `${replayFrom ?? 'live'}:${replayTo ?? 'live'}`;
  const [status, setStatus] = useState('connecting');
  const [subscribed, setSubscribed] = useState(subscription);

  // React's documented "adjust state when a prop changes" pattern, and what
  // react-hooks/set-state-in-effect is asking for. Resetting the status inside the effect instead
  // schedules a second render pass, and the frame in between paints the PREVIOUS stream's status
  // against the new window — a visible flash of 'connected' over a stream that no longer exists.
  // Doing it during render means the two never disagree.
  if (subscribed !== subscription) {
    setSubscribed(subscription);
    setStatus('connecting');
  }

  useEffect(() => {
    clear();

    let sseSource: EventSource | null = null;
    let isSnapshotLoaded = false;
    let snapshotCursor = BigInt(0);
    // One controller per effect run. Cleanup aborts it, so every fetch below is bounded by the
    // lifetime of the effect that started it and cannot write into a later run's store.
    const controller = new AbortController();

    const initStream = async (startCursor: bigint) => {
      sseSource = new EventSource(STREAM_URL);

      sseSource.addEventListener('movement', (e: MessageEvent) => {
        const data = JSON.parse(e.data) as P2PEvent;
        if (!isSnapshotLoaded) {
          bufferRef.current.push(data);
        } else {
          if (BigInt(data.sequence_engine_key) > snapshotCursor) {
            addEvents([data]);
          }
        }
      });

      sseSource.addEventListener('watermark', (e: MessageEvent) => {
        const data = JSON.parse(e.data);
        setWatermark(BigInt(data.resolved_ts));
      });

      sseSource.onopen = async () => {
        setStatus('connected');
        // Fetch historical snapshot up to current moment
        try {
          const history = await fetchWindow(replayURL(startCursor, 0), controller.signal);

          if (history && history.length > 0) {
            snapshotCursor = BigInt(history[history.length - 1].sequence_engine_key);
            addEvents(history);
          } else {
            snapshotCursor = startCursor;
          }

          // Process buffered live events
          const validBuffer = bufferRef.current.filter(
            ev => BigInt(ev.sequence_engine_key) > snapshotCursor,
          );
          if (validBuffer.length > 0) {
            addEvents(validBuffer);
          }
          bufferRef.current = [];
          isSnapshotLoaded = true;
        } catch (err) {
          report('Failed to fetch snapshot', err);
        }
      };

      sseSource.onerror = () => {
        setStatus('error');
      };
    };

    if (replayFrom !== null && replayTo !== null) {
      // Replay mode: Fetch history by range. Do not open SSE unless they rejoin live.
      fetchWindow(replayURL(replayFrom, replayTo), controller.signal)
        .then((history: P2PEvent[]) => {
          if (history && history.length > 0) {
            addEvents(history);
            setWatermark(BigInt(history[history.length - 1].sequence_engine_key));
          }
          setStatus('replay_finished');
        })
        .catch(err => {
          report('Failed to fetch replay history', err);
          // An abort means a newer effect run is already in charge; leaving the status alone keeps
          // this one from stamping 'error' over the state its successor is establishing.
          if (!isAbort(err)) setStatus('error');
        });
    } else {
      // initStream is async, so its rejection is a promise rejection, not a throw the effect body
      // can see. Unhandled, it surfaces as an uncaught error attributed to this useEffect frame —
      // which is what made the original failure so hard to place.
      initStream(BigInt(0)).catch(err => {
        report('Failed to open stream', err);
        if (!isAbort(err)) setStatus('error');
      });
    }

    return () => {
      controller.abort();
      if (sseSource) sseSource.close();
    };
  }, [replayFrom, replayTo, addEvents, clear, setWatermark]);

  return { status };
}
