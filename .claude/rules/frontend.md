---
paths:
  - "frontend/**"
  - "**/*.tsx"
  - "**/*.jsx"
  - "**/*.css"
---

# Frontend

## The stack that exists — don't add to it

Next.js 16 (App Router), React 19, TypeScript, Tailwind CSS v4 via `@tailwindcss/postcss`, and
zustand for state. There is no component library, no animation library, and no chart library, and
adding one is a decision, not a detail — raise it before installing. Build with Tailwind utilities
and plain React; charts and motion are hand-rolled unless agreed otherwise.

Layout lives in `src/app/`, shared pieces in `src/components/`, hooks in `src/hooks/`, the store in
`src/store/`, config in `src/lib/`.

## Data contract with the backend

- Transport is **SSE**, by deliberate ADR. Don't reach for WebSockets, polling, or a fetch loop.
- `sequence_engine_key` is a **STRING end-to-end**. Never parse it into a JS `number` — it exceeds
  `float64` precision and will silently corrupt. Compare and sort it as a string.
- Render is gated by the changefeed `resolved` watermark. Don't paint rows ahead of the watermark
  just because the event arrived; that is how the dashboard starts lying.

## Design Tokens

Use the tokens in `src/app/globals.css` and the Tailwind theme. Never hardcode raw colors, spacing,
or radii in a component. If a value has no token and will be used twice, add the token first.

## Layout

- CSS Grid for 2D, Flexbox for 1D. Use `gap`, not margin hacks.
- Semantic HTML: `<header>`, `<nav>`, `<main>`, `<section>`, `<article>`, `<footer>`.
- Mobile-first. Touch targets: minimum 44x44px.

## Accessibility (non-negotiable)

- All interactive elements keyboard-accessible.
- Images: meaningful `alt` text. Decorative: `alt=""`.
- Form inputs: associated `<label>` or `aria-label`.
- Contrast: 4.5:1 normal text, 3:1 large text.
- Visible focus indicators. Never `outline: none` without a replacement.
- Color never the sole indicator — a status pill needs a label, not just a hue.
- `aria-live` for streaming updates. Respect `prefers-reduced-motion` and `prefers-color-scheme`.

## Performance

- A live event stream is the hot path: buffer and batch renders (see `useEventBuffer.ts`) rather
  than setting state per event.
- Virtualize lists at 100+ rows. The ledger grows without bound.
- Animations: `transform` and `opacity` only.
- Images: `loading="lazy"` below the fold, explicit `width`/`height`. Fonts: `font-display: swap`.
- Never import a whole library for one function.
