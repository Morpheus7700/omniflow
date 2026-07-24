import { useStore } from '@/store';
import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Activity, Clock, Database, FastForward, Play, Server, AlertTriangle } from 'lucide-react';

interface HUDProps {
  onReplay: (from: bigint | null, to: bigint | null) => void;
}

export function HUD({ onReplay }: HUDProps) {
  const events = useStore(state => state.events);
  const watermark = useStore(state => state.watermark);

  const visibleEvents = events.filter(e => BigInt(e.sequence_engine_key) <= watermark);
  const totalValue = visibleEvents.reduce((acc, e) => acc + (e.metrics?.value || 0), 0);
  const totalBreaches = visibleEvents.filter(e => e.metrics?.sla_breached || e.status === 'FAILURE').length;
  
  const [replayStart, setReplayStart] = useState('0');
  const [replayEnd, setReplayEnd] = useState('1000');
  const [isReplaying, setIsReplaying] = useState(false);

  const handleReplayClick = () => {
    onReplay(BigInt(replayStart), BigInt(replayEnd));
    setIsReplaying(true);
  };

  const handleGoLiveClick = () => {
    onReplay(null, null);
    setIsReplaying(false);
  };

  // Keep only the last 5 events for the live feed to prevent clutter
  const liveEventsFeed = [...visibleEvents].reverse().slice(0, 5);

  return (
    <div className="absolute inset-0 pointer-events-none p-6 flex justify-between z-10">
      
      {/* Left Panel - Metrics & Controls */}
      <motion.div 
        initial={{ x: -50, opacity: 0 }}
        animate={{ x: 0, opacity: 1 }}
        transition={{ type: 'spring', stiffness: 100, damping: 20 }}
        className="w-96 flex flex-col gap-6 h-full pointer-events-auto"
      >
        {/* Header */}
        <div className="flex items-center gap-3 pb-4 border-b border-white/10">
          <div className="w-8 h-8 rounded bg-zinc-800 flex items-center justify-center border border-white/5 shadow-inner">
            <Activity className="w-4 h-4 text-zinc-300" />
          </div>
          <div>
            <h1 className="text-sm font-semibold tracking-widest text-zinc-100 uppercase">OmniFlow</h1>
            <p className="text-xs text-zinc-500 font-mono tracking-tight">ORCHESTRATOR HUD v2</p>
          </div>
        </div>

        {/* Global Metrics */}
        <div className="flex flex-col gap-3">
          <MetricCard 
            title="SYSTEM WATERMARK" 
            value={watermark.toString()} 
            icon={<Clock className="w-4 h-4 text-zinc-400" />} 
          />
          <MetricCard 
            title="TOTAL VALUE PROCESSED" 
            value={`$${totalValue.toLocaleString(undefined, {minimumFractionDigits: 2})}`} 
            icon={<Database className="w-4 h-4 text-zinc-400" />} 
            highlight="text-zinc-100"
          />
          <MetricCard 
            title="SLA BREACHES / FAILURES" 
            value={totalBreaches.toString()} 
            icon={<AlertTriangle className="w-4 h-4 text-zinc-400" />} 
            highlight={totalBreaches > 0 ? "text-red-400" : "text-zinc-400"}
          />
        </div>

        {/* Scrub Controls */}
        <div className="mt-auto bg-[#09090b]/80 backdrop-blur-md border border-white/10 p-5 rounded-lg flex flex-col gap-4 shadow-2xl">
          <div className="flex items-center justify-between">
            <h2 className="text-xs font-semibold tracking-wider text-zinc-400 uppercase">Temporal Controls</h2>
            {isReplaying ? (
              <span className="text-[10px] bg-amber-900/30 text-amber-400 border border-amber-500/20 px-2 py-0.5 rounded animate-pulse">REPLAY MODE</span>
            ) : (
              <span className="text-[10px] bg-green-900/30 text-green-400 border border-green-500/20 px-2 py-0.5 rounded">LIVE STREAM</span>
            )}
          </div>
          
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <label className="text-[10px] text-zinc-500 uppercase">From HLC</label>
              <input 
                type="text" 
                value={replayStart} 
                onChange={e => setReplayStart(e.target.value)}
                className="bg-zinc-900/80 border border-white/10 rounded px-3 py-1.5 text-zinc-200 font-mono text-xs focus:outline-none focus:border-zinc-500 transition-colors"
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <label className="text-[10px] text-zinc-500 uppercase">To HLC (0=Max)</label>
              <input 
                type="text" 
                value={replayEnd} 
                onChange={e => setReplayEnd(e.target.value)}
                className="bg-zinc-900/80 border border-white/10 rounded px-3 py-1.5 text-zinc-200 font-mono text-xs focus:outline-none focus:border-zinc-500 transition-colors"
              />
            </div>
          </div>
          
          <div className="flex gap-2 mt-2">
            <button 
              onClick={handleReplayClick}
              className="flex-1 flex items-center justify-center gap-2 bg-zinc-800 hover:bg-zinc-700 text-zinc-200 text-xs font-semibold px-4 py-2 rounded transition-colors border border-white/5"
            >
              <FastForward className="w-3.5 h-3.5" />
              Scrub
            </button>
            <button 
              onClick={handleGoLiveClick}
              className="flex-1 flex items-center justify-center gap-2 bg-zinc-200 hover:bg-white text-zinc-900 text-xs font-semibold px-4 py-2 rounded transition-colors shadow-sm"
            >
              <Play className="w-3.5 h-3.5" />
              Live
            </button>
          </div>
        </div>
      </motion.div>

      {/* Right Panel - Live Event Feed */}
      <motion.div 
        initial={{ x: 50, opacity: 0 }}
        animate={{ x: 0, opacity: 1 }}
        transition={{ type: 'spring', stiffness: 100, damping: 20, delay: 0.1 }}
        className="w-80 flex flex-col h-full pointer-events-auto"
      >
        <div className="bg-[#09090b]/80 backdrop-blur-md border border-white/10 rounded-lg p-5 flex flex-col h-[500px] overflow-hidden shadow-2xl mt-auto mb-[22px]">
          <div className="flex items-center gap-2 pb-4 border-b border-white/5 mb-4 shrink-0">
            <Server className="w-4 h-4 text-zinc-400" />
            <h2 className="text-xs font-semibold tracking-wider text-zinc-300 uppercase">Event Ledger</h2>
          </div>
          
          <div className="flex flex-col gap-3 flex-1 overflow-y-auto pr-2 custom-scrollbar relative">
            <AnimatePresence initial={false}>
              {liveEventsFeed.map((event) => (
                <motion.div
                  key={event.sequence_engine_key}
                  initial={{ opacity: 0, y: -20, scale: 0.95 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.9, transition: { duration: 0.1 } }}
                  layout
                  className="bg-zinc-900/60 border border-white/5 p-3 rounded text-xs flex flex-col gap-2 shrink-0"
                >
                  <div className="flex justify-between items-center border-b border-white/5 pb-2">
                    <span className="font-mono text-[10px] text-zinc-500">{event.sequence_engine_key}</span>
                    <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold uppercase tracking-wider ${
                      event.status === 'COMPLETED' ? 'bg-green-900/30 text-green-400 border border-green-500/20' :
                      event.status === 'FAILURE' ? 'bg-red-900/30 text-red-400 border border-red-500/20' :
                      'bg-zinc-800 text-zinc-300 border border-white/5'
                    }`}>
                      {event.status || 'PROCESSED'}
                    </span>
                  </div>
                  <div className="text-zinc-200 font-medium truncate">
                    {event.stage || 'System Event'}
                  </div>
                  {event.metrics?.value != null && (
                    <div className="text-zinc-500 font-mono text-[10px]">
                      Value: ${event.metrics.value.toLocaleString(undefined, {minimumFractionDigits: 2})}
                    </div>
                  )}
                </motion.div>
              ))}
            </AnimatePresence>
            
            {liveEventsFeed.length === 0 && (
              <div className="absolute inset-0 flex items-center justify-center text-zinc-600 text-xs font-mono uppercase tracking-widest">
                Awaiting events...
              </div>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
}

function MetricCard({ title, value, icon, highlight = "text-zinc-200" }: { title: string, value: string, icon: React.ReactNode, highlight?: string }) {
  return (
    <div className="bg-[#09090b]/80 backdrop-blur-md border border-white/10 p-4 rounded-lg flex flex-col gap-2 relative overflow-hidden group shadow-lg">
      <div className="absolute top-0 left-0 w-1 h-full bg-zinc-800 group-hover:bg-zinc-600 transition-colors" />
      <div className="flex items-center gap-2 text-zinc-500 pl-2">
        {icon}
        <span className="text-[10px] font-semibold tracking-widest uppercase">{title}</span>
      </div>
      <div className={`text-2xl font-mono pl-2 tracking-tight ${highlight}`}>
        {value}
      </div>
    </div>
  );
}
