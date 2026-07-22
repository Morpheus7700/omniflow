'use client';

import { useState } from 'react';
import { Canvas } from '@react-three/fiber';
import { NetworkGraph } from '@/components/NetworkGraph';
import { HUD } from '@/components/HUD';
import { useEventBuffer } from '@/hooks/useEventBuffer';

export default function Home() {
  const [replayRange, setReplayRange] = useState<{from: bigint | null, to: bigint | null}>({ from: null, to: null });
  const { status } = useEventBuffer(replayRange.from, replayRange.to);

  return (
    <main className="w-screen h-screen bg-slate-950 overflow-hidden relative">
      <HUD onReplay={(from, to) => setReplayRange({ from, to })} />
      
      {status === 'error' && (
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-rose-900/90 text-white px-6 py-3 rounded-lg z-50 pointer-events-none">
          Connection Error: SSE disconnected.
        </div>
      )}

      <Canvas
        frameloop="always"
        camera={{ position: [0, 12, 12], fov: 45 }}
      >
        <NetworkGraph />
      </Canvas>
    </main>
  );
}
