'use client';

import { useState } from 'react';
import { HUD } from '@/components/HUD';
import { useEventBuffer } from '@/hooks/useEventBuffer';
import { motion, AnimatePresence } from 'framer-motion';
import { Activity, ShieldAlert, Cpu } from 'lucide-react';

export default function Home() {
  const [replayRange, setReplayRange] = useState<{from: bigint | null, to: bigint | null}>({ from: null, to: null });
  const { status } = useEventBuffer(replayRange.from, replayRange.to);

  return (
    <main className="w-screen h-screen bg-[#060a10] text-zinc-300 overflow-hidden font-sans flex">
      
      {/* Sidebar Navigation */}
      <aside className="w-16 md:w-64 border-r border-white/5 bg-[#0a0e1a]/80 backdrop-blur-xl flex flex-col items-center md:items-start py-6 transition-all duration-300 z-20">
        <div className="flex items-center gap-3 px-0 md:px-6 mb-12">
          <div className="w-10 h-10 rounded bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center shadow-[0_0_15px_rgba(0,240,255,0.1)]">
            <Cpu className="w-5 h-5 text-cyan-400" />
          </div>
          <h1 className="hidden md:block text-lg font-bold tracking-widest text-white uppercase">OmniFlow</h1>
        </div>
        
        <nav className="flex flex-col gap-4 w-full px-4 md:px-6">
          <NavItem icon={<Activity className="w-5 h-5" />} label="Orchestration" active />
          <NavItem icon={<ShieldAlert className="w-5 h-5" />} label="System Health" />
        </nav>
      </aside>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col h-full overflow-hidden relative">
        {/* Header */}
        <header className="h-16 border-b border-white/5 bg-[#0d1117]/80 backdrop-blur-md flex items-center px-8 justify-between z-10 shrink-0">
          <div className="flex items-center gap-4">
            <span className="text-xs font-mono text-cyan-400/70 tracking-widest uppercase">/ Platform / Orchestration / Live View</span>
          </div>
          
          {/* Connection Error Toast positioned in header */}
          <AnimatePresence>
            {status === 'error' && (
              <motion.div 
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="bg-fuchsia-900/40 border border-[#ff00e5]/30 text-[#ff00e5] px-4 py-1.5 rounded text-xs font-mono tracking-wider flex items-center gap-3"
              >
                <div className="w-1.5 h-1.5 rounded-full bg-[#ff00e5] animate-pulse" />
                STREAM DISCONNECTED
              </motion.div>
            )}
          </AnimatePresence>
        </header>

        {/* Dashboard Grid Container */}
        <div className="flex-1 p-6 md:p-8 overflow-y-auto custom-scrollbar relative bg-[url('https://www.transparenttextures.com/patterns/cubes.png')] bg-repeat">
           <HUD onReplay={(from, to) => setReplayRange({ from, to })} />
        </div>
      </div>
    </main>
  );
}

function NavItem({ icon, label, active = false }: { icon: React.ReactNode, label: string, active?: boolean }) {
  return (
    <div className={`flex items-center gap-3 p-3 rounded-lg cursor-pointer transition-all ${active ? 'bg-cyan-500/10 text-cyan-400 border border-cyan-500/20' : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/5'}`}>
      {icon}
      <span className="hidden md:block text-xs font-semibold tracking-wider uppercase">{label}</span>
    </div>
  );
}
