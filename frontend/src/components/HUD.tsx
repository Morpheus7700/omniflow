import { useStore } from '@/store';
import { useState } from 'react';

interface HUDProps {
  onReplay: (from: bigint | null, to: bigint | null) => void;
}

export function HUD({ onReplay }: HUDProps) {
  const events = useStore(state => state.events);
  const watermark = useStore(state => state.watermark);

  // Filter visible metrics
  const visibleEvents = events.filter(e => BigInt(e.sequence_engine_key) <= watermark);
  const totalValue = visibleEvents.reduce((acc, e) => acc + (e.metrics?.value || 0), 0);
  const totalBreaches = visibleEvents.filter(e => e.metrics?.sla_breached || e.status === 'FAILURE').length;
  
  const [replayStart, setReplayStart] = useState('0');
  const [replayEnd, setReplayEnd] = useState('1000');

  const handleReplayClick = () => {
    onReplay(BigInt(replayStart), BigInt(replayEnd));
  };

  const handleGoLiveClick = () => {
    onReplay(null, null); // returns to live subscribe-then-snapshot
  };

  return (
    <div className="absolute top-0 left-0 w-full h-full pointer-events-none p-8 flex flex-col justify-between z-10">
      
      {/* Top Bar - Metrics */}
      <div className="flex gap-8 items-start pointer-events-auto">
        <div className="bg-slate-900/80 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-2xl">
          <h2 className="text-slate-400 text-sm font-semibold uppercase tracking-wider mb-2">Live Watermark</h2>
          <div className="text-3xl font-mono text-emerald-400">{watermark.toString()}</div>
        </div>

        <div className="bg-slate-900/80 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-2xl">
          <h2 className="text-slate-400 text-sm font-semibold uppercase tracking-wider mb-2">Total Value Processed</h2>
          <div className="text-3xl font-mono text-white">${totalValue.toLocaleString(undefined, {minimumFractionDigits: 2})}</div>
        </div>

        <div className="bg-slate-900/80 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-2xl">
          <h2 className="text-slate-400 text-sm font-semibold uppercase tracking-wider mb-2">SLA Breaches / Failures</h2>
          <div className="text-3xl font-mono text-rose-400">{totalBreaches}</div>
        </div>
      </div>

      {/* Bottom Bar - Controls */}
      <div className="bg-slate-900/80 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-2xl max-w-xl pointer-events-auto">
        <h2 className="text-slate-400 text-sm font-semibold uppercase tracking-wider mb-4">Time Travel / Scrub</h2>
        <div className="flex gap-4 items-end">
          <div className="flex flex-col gap-2">
            <label className="text-xs text-slate-500">From Seq</label>
            <input 
              type="text" 
              value={replayStart} 
              onChange={e => setReplayStart(e.target.value)}
              className="bg-slate-800 border border-slate-600 rounded p-2 text-white font-mono text-sm w-32"
            />
          </div>
          <div className="flex flex-col gap-2">
            <label className="text-xs text-slate-500">To Seq (0 = Max)</label>
            <input 
              type="text" 
              value={replayEnd} 
              onChange={e => setReplayEnd(e.target.value)}
              className="bg-slate-800 border border-slate-600 rounded p-2 text-white font-mono text-sm w-32"
            />
          </div>
          <button 
            onClick={handleReplayClick}
            className="bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2 rounded font-semibold transition-colors"
          >
            Replay Range
          </button>
          <button 
            onClick={handleGoLiveClick}
            className="bg-emerald-600 hover:bg-emerald-500 text-white px-6 py-2 rounded font-semibold transition-colors ml-4"
          >
            Go Live (Re-Sync)
          </button>
        </div>
      </div>
    </div>
  );
}
