import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Ledger, WindowedRows, ROW_HEIGHT } from './Ledger';
import { useStore, type P2PEvent } from '@/store';

const ev = (key: string, extra: Partial<P2PEvent> = {}): P2PEvent => ({
  aggregate_id: `agg-${key}`,
  stage: 'PO_CREATED',
  status: 'SUCCESS',
  sequence_engine_key: key,
  occurred_at: '2026-09-22T00:00:00Z',
  ...extra,
});

beforeEach(() => {
  useStore.getState().clear();
  // jsdom has no layout; ResizeObserver is a browser API the windowed list subscribes to.
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      disconnect() {}
      unobserve() {}
    },
  );
});

describe('Ledger', () => {
  it('draws the settlement line between in-flight and settled rows', () => {
    useStore.getState().addEvents([ev('1'), ev('2'), ev('3')]);
    useStore.getState().setWatermark(2n);
    render(<Ledger />);

    expect(screen.getByRole('separator', { name: /settlement boundary/i })).toHaveTextContent('2');
    const inFlight = screen.getByLabelText('In flight');
    expect(inFlight).toHaveTextContent('agg-3');
    expect(inFlight).not.toHaveTextContent('agg-2');
    const settled = screen.getByLabelText('Settled');
    expect(settled).toHaveTextContent('agg-2');
    expect(settled).toHaveTextContent('agg-1');
  });

  it('marks a FAILURE as an exception in words, not only in colour', () => {
    useStore.getState().addEvents([ev('1', { status: 'FAILURE' }), ev('2', { status: 'PENDING' })]);
    useStore.getState().setWatermark(5n);
    render(<Ledger />);
    expect(screen.getByText(/order raised · exception/i)).toBeInTheDocument();
    expect(screen.getByText(/awaiting approval/i)).toBeInTheDocument();
  });

  it('announces counts through a live region rather than reading the whole ledger', () => {
    useStore.getState().addEvents([ev('1'), ev('2')]);
    useStore.getState().setWatermark(1n);
    render(<Ledger />);
    const live = document.querySelector('[aria-live="polite"]');
    expect(live).toHaveTextContent('1 in flight, 1 settled, settled through 1');
  });

  it('shows the empty state without developer instructions', () => {
    render(<Ledger />);
    expect(screen.getByText(/no orders on the wire yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/go run/)).not.toBeInTheDocument();
  });
});

describe('WindowedRows', () => {
  it('renders only the rows near the viewport, with the full height reserved', () => {
    const rows: P2PEvent[] = [];
    for (let i = 5000; i >= 1; i--) rows.push(ev(String(i)));
    render(<WindowedRows rows={rows} />);

    const list = screen.getByTestId('settled-window');
    const items = list.querySelectorAll('[role="listitem"]');
    // ~13 visible + overscan, never thousands.
    expect(items.length).toBeGreaterThan(5);
    expect(items.length).toBeLessThan(40);
    const spacer = list.firstElementChild as HTMLElement;
    expect(spacer.style.height).toBe(`${5000 * ROW_HEIGHT}px`);
    // Position semantics survive the windowing.
    expect(items[0]).toHaveAttribute('aria-posinset', '1');
    expect(items[0]).toHaveAttribute('aria-setsize', '5000');
  });
});
