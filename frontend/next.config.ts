import type { NextConfig } from "next";

/**
 * Security headers on every response. None were set before: a browser given no instructions will
 * sniff content types, allow the page to be framed, and send the full URL as a referrer.
 *
 * The CSP is strict for a dashboard that loads nothing third-party: fonts are self-hosted by
 * next/font, the only network peers are the page's own origin and the gateway (connect-src is
 * widened to the gateway origin at build time from NEXT_PUBLIC_API_BASE, which is inlined the
 * same way the client bundle inlines it). 'unsafe-inline' for styles is what Tailwind's runtime
 * class injection needs in dev and what Next's hydration styles need in prod; scripts stay
 * nonce-free because Next emits its own inline bootstrap only under 'self' + the hashes it
 * manages, and this app registers no inline handlers.
 */
const apiOrigin = (() => {
  try {
    return new URL(process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8081").origin;
  } catch {
    return "http://localhost:8081";
  }
})();

// React's development build needs eval() for its debugging features (callstack reconstruction,
// Fast Refresh); the production build never does. Widening script-src only when NODE_ENV is not
// production keeps the shipped policy strict.
const isDev = process.env.NODE_ENV !== "production";

const csp = [
  "default-src 'self'",
  `script-src 'self' 'unsafe-inline'${isDev ? " 'unsafe-eval'" : ""}`,
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data:",
  "font-src 'self'",
  `connect-src 'self' ${apiOrigin}`,
  "frame-ancestors 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "object-src 'none'",
].join("; ");

const securityHeaders = [
  { key: "Content-Security-Policy", value: csp },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=(), payment=()" },
  // HSTS is only meaningful over TLS, and the compose stack is plain http on localhost; a
  // deployment fronted by TLS should keep it. Browsers ignore it on http, so it is safe to emit.
  { key: "Strict-Transport-Security", value: "max-age=31536000; includeSubDomains" },
];

const nextConfig: NextConfig = {
  output: "standalone",
  // No `X-Powered-By: Next.js`: version fingerprinting is free reconnaissance.
  poweredByHeader: false,
  // No next/image is used anywhere; disabling the optimizer removes the AVIF/sharp code path
  // (the subject of two critical advisories in 2026) from the image entirely.
  images: { unoptimized: true },
  // Opening the dev server by IP (127.0.0.1) instead of `localhost` is otherwise refused as a
  // cross-origin dev-resource request, which silently leaves the page unhydrated.
  allowedDevOrigins: ["127.0.0.1"],
  // `next dev` regenerates frontend/AGENTS.md on every start; it is a tracked file, so every dev
  // session dirtied the tree. The repo's agent rules live in CLAUDE.md and .claude/rules.
  agentRules: false,
  async headers() {
    return [{ source: "/(.*)", headers: securityHeaders }];
  },
};

export default nextConfig;
