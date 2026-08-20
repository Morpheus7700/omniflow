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

/**
 * The gateway caps a replay response. Asking for more than MAX_REPLAY_LIMIT is rejected with 400
 * rather than clamped, so this value must stay in step with maxReplayLimit in
 * services/viz-gateway/internal/api/replay.go.
 */
export const MAX_REPLAY_LIMIT = 5000;

/**
 * Cursors accept number as well as bigint/string: callers legitimately pass the literal 0 to mean
 * "from the beginning", and HLC keys arrive as bigint. All three stringify identically in the query.
 *
 * `limit` is optional and defaults, server-side, to 500. It is worth passing explicitly whenever the
 * caller cares about completeness: the response is truncated at the limit with no marker, so a full
 * page of results means "there may be more", not "that is everything". Page by re-requesting from
 * the last sequence key you received.
 */
export function replayURL(
  fromSeq: bigint | number | string,
  toSeq: bigint | number | string,
  limit?: number,
): string {
  const base = `${API_BASE}/api/replay?from_seq=${fromSeq}&to_seq=${toSeq}`;
  return limit === undefined ? base : `${base}&limit=${limit}`;
}
