import { useEffect, useState, useRef } from 'react';
import { useStore, P2PEvent } from '@/store';

export function useEventBuffer(replayFrom: bigint | null = null, replayTo: bigint | null = null) {
  const addEvents = useStore(state => state.addEvents);
  const setWatermark = useStore(state => state.setWatermark);
  const clear = useStore(state => state.clear);
  const [status, setStatus] = useState('connecting');
  const bufferRef = useRef<P2PEvent[]>([]);

  useEffect(() => {
    clear();
    setStatus('connecting');

    let sseSource: EventSource | null = null;
    let isSnapshotLoaded = false;
    let snapshotCursor = BigInt(0);

    const initStream = async (startCursor: bigint) => {
      sseSource = new EventSource('http://localhost:8080/api/stream');

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
          const res = await fetch(`http://localhost:8080/api/replay?from_seq=${startCursor}&to_seq=0`);
          const history = (await res.json()) as P2PEvent[];
          
          if (history && history.length > 0) {
            snapshotCursor = BigInt(history[history.length - 1].sequence_engine_key);
            addEvents(history);
          } else {
            snapshotCursor = startCursor;
          }
          
          // Process buffered live events
          const validBuffer = bufferRef.current.filter(ev => BigInt(ev.sequence_engine_key) > snapshotCursor);
          if (validBuffer.length > 0) {
            addEvents(validBuffer);
          }
          bufferRef.current = [];
          isSnapshotLoaded = true;
        } catch (err) {
          console.error('Failed to fetch snapshot', err);
        }
      };

      sseSource.onerror = () => {
        setStatus('error');
      };
    };

    if (replayFrom !== null && replayTo !== null) {
      // Replay mode: Fetch history by range. Do not open SSE unless they rejoin live.
      fetch(`http://localhost:8080/api/replay?from_seq=${replayFrom}&to_seq=${replayTo}`)
        .then(res => res.json())
        .then((history: P2PEvent[]) => {
          if (history && history.length > 0) {
            addEvents(history);
            setWatermark(BigInt(history[history.length - 1].sequence_engine_key));
          }
          setStatus('replay_finished');
        });
    } else {
      initStream(BigInt(0));
    }

    return () => {
      if (sseSource) sseSource.close();
    };
  }, [replayFrom, replayTo, addEvents, clear, setWatermark]);

  return { status };
}
