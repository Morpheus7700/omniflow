'use client';

import { useState } from 'react';
import { Ledger } from '@/components/Ledger';
import { Rail } from '@/components/Rail';
import { useEventBuffer } from '@/hooks/useEventBuffer';

export default function Home() {
  const [range, setRange] = useState<{ from: bigint | null; to: bigint | null }>({
    from: null,
    to: null,
  });
  const { status } = useEventBuffer(range.from, range.to);
  const replaying = range.from !== null;

  return (
    <main className="mx-auto flex min-h-screen max-w-[1400px] flex-col px-6 md:px-10">
      <Masthead />

      {/*
        The thesis, stated once at the top in the display face. A buyer should learn what is
        different about this system before they look at a single figure.
      */}
      <section className="border-b border-[var(--rule)] py-10 md:py-14">
        {/*
          h1, not h2: this is the page's one true heading. The document previously started at h2,
          which leaves a screen reader with no top-level landmark to jump to and reads as a
          fragment of some larger page. Purely semantic — the type scale is set by className, so
          nothing moves visually.
        */}
        <h1 className="max-w-[24ch] font-serif text-[2rem] leading-[1.15] md:text-[2.75rem]">
          Every order carries a provable position in one global order.
        </h1>
        <p className="mt-4 max-w-[62ch] text-[15px] leading-relaxed text-[var(--muted)]">
          The line below marks settlement. Records above it are still in flight; records beneath it
          have been resolved by the changefeed and their order can no longer change — so they can be
          reconciled, audited, and replayed exactly as they happened.
        </p>
      </section>

      <div className="grid flex-1 gap-10 py-8 md:grid-cols-[minmax(0,260px)_minmax(0,1fr)] md:gap-14">
        <aside className="md:border-r md:border-[var(--rule)] md:pr-10">
          <Rail
            status={status}
            replaying={replaying}
            onReplay={(from, to) => setRange({ from, to })}
            onGoLive={() => setRange({ from: null, to: null })}
          />
        </aside>

        <Ledger />
      </div>

      <footer className="border-t border-[var(--rule)] py-6 text-[12px] text-[var(--muted)]">
        Ordering is established by a hybrid logical clock and carried end to end as an exact integer.
        Settlement follows the CockroachDB changefeed resolved watermark.
      </footer>
    </main>
  );
}

function Masthead() {
  return (
    <header className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-2 border-b border-[var(--ink)] py-5">
      <div className="flex items-baseline gap-3">
        <span className="font-serif text-xl tracking-tight">
          OmniFlow
        </span>
        <span className="text-[13px] text-[var(--muted)]">Procure-to-pay orchestration</span>
      </div>
      <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">
        Live operations
      </span>
    </header>
  );
}
