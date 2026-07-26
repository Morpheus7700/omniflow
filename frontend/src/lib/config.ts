/**
 * Single source of truth for the viz-gateway origin the browser talks to.
 *
 * PORT 8081, NOT 8080. docker-compose maps viz-gateway as `8081:8080` — port 8080 on the host is
 * CockroachDB's admin UI. Pointing the browser at 8080 aims it at the database console instead of
 * the event gateway, which is how this was previously misconfigured.
 *
 * `NEXT_PUBLIC_*` is inlined at BUILD time and frozen into the bundle, so one image cannot be
 * repointed at a different gateway afterwards — it must be rebuilt per environment (see
 * node_modules/next/dist/docs/01-app/02-guides/environment-variables.md). That is why the Dockerfile
 * declares this as a build ARG and not a runtime ENV on the runner stage.
 *
 * Referenced as a direct `process.env.NEXT_PUBLIC_…` expression on purpose: Next.js only inlines
 * direct references. Destructuring or dynamic indexing silently yields undefined in the browser.
 */
export const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? 'http://localhost:8081';

export const STREAM_URL = `${API_BASE}/api/stream`;

export function replayURL(fromSeq: bigint | string, toSeq: bigint | string): string {
  return `${API_BASE}/api/replay?from_seq=${fromSeq}&to_seq=${toSeq}`;
}
