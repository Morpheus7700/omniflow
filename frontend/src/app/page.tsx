'use client';

import { useState } from 'react';
import { Canvas } from '@react-three/fiber';
import { NetworkGraph } from '@/components/NetworkGraph';
import { HUD } from '@/components/HUD';
import { useEventBuffer } from '@/hooks/useEventBuffer';
import { motion, AnimatePresence } from 'framer-motion';

export default function Home() {
  const [replayRange, setReplayRange] = useState<{from: bigint | null, to: bigint | null}>({ from: null, to: null });
  const { status } = useEventBuffer(replayRange.from, replayRange.to);

  return (
    <main className="w-screen h-screen bg-[#09090b] overflow-hidden relative font-sans">
      
      {/* 3D Canvas Background layer */}
      <div className="absolute inset-0 z-0">
        <Canvas
          frameloop="always"
          camera={{ position: [0, 12, 12], fov: 45 }}
        >
          <NetworkGraph />
        </Canvas>
      </div>

      {/* Connection Error Overlay */}
      <AnimatePresence>
        {status === 'error' && (
          <motion.div 
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            className="absolute top-8 left-1/2 -translate-x-1/2 bg-red-950/80 backdrop-blur-md border border-red-900/50 text-red-200 px-6 py-3 rounded-lg z-50 pointer-events-none shadow-2xl flex items-center gap-3"
          >
            <div className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
            <span className="font-mono text-sm tracking-wide">SYSTEM DISCONNECTED: STREAM LOST</span>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Enterprise HUD Layer */}
      <HUD onReplay={(from, to) => setReplayRange({ from, to })} />
      
    </main>
  );
}
