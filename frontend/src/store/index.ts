import { create } from 'zustand';

export type Stage = 'PO_CREATED' | 'VENDOR_CONFIRMED' | 'APPROVED' | 'IN_TRANSIT' | 'RECEIVED';

export interface P2PEvent {
  aggregate_id: string;
  stage: Stage;
  status: string;
  sequence_engine_key: string;
  occurred_at: string;
  trace_parent?: string;
  metrics?: {
    value?: number;
    sla_breached?: boolean;
  };
}

interface AppState {
  events: P2PEvent[];
  watermark: bigint;
  addEvents: (newEvents: P2PEvent[]) => void;
  setWatermark: (wm: bigint) => void;
  clear: () => void;
}

export const useStore = create<AppState>((set) => ({
  events: [],
  watermark: BigInt(0),
  addEvents: (newEvents) =>
    set((state) => {
      // Dedup logic based on aggregate_id + sequence_engine_key
      const existing = new Set(state.events.map(e => e.aggregate_id + e.sequence_engine_key));
      const filtered = newEvents.filter(e => !existing.has(e.aggregate_id + e.sequence_engine_key));
      
      const merged = [...state.events, ...filtered];
      // Keep them sorted by HLC
      merged.sort((a, b) => {
        const diff = BigInt(a.sequence_engine_key) - BigInt(b.sequence_engine_key);
        return diff > BigInt(0) ? 1 : diff < BigInt(0) ? -1 : 0;
      });
      return { events: merged };
    }),
  setWatermark: (wm) => set({ watermark: wm }),
  clear: () => set({ events: [], watermark: BigInt(0) }),
}));
