/** Route loading state: the masthead's silhouette, so the page does not flash from blank to full. */
export default function Loading() {
  return (
    <main className="mx-auto flex min-h-screen max-w-[1400px] flex-col px-6 md:px-10" aria-busy="true">
      <div className="border-b border-[var(--ink)] py-5">
        <span className="font-serif text-xl tracking-tight">OmniFlow</span>
      </div>
      <p className="py-10 text-sm text-[var(--muted)]" role="status">
        Opening the ledger…
      </p>
    </main>
  );
}
