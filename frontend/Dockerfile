# syntax=docker/dockerfile:1
# omniflow-frontend — the Next.js dashboard; build context is ./frontend (see .dockerignore here).
#
# Node 24 is the Active LTS line. Node 26 is "Current" until it enters LTS (scheduled for late 2026),
# and a production image tracks LTS, not Current. Pinned by digest for the same reason the Go images
# are (see any services/*/Dockerfile): a tag is a moving pointer; a digest is a build input.
FROM node:26-alpine@sha256:dbaa92e5758cbbcf85d65d5403fdb530fe3442cbe8c6dbfb7ef23365450d5070 AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund

FROM node:26-alpine@sha256:dbaa92e5758cbbcf85d65d5403fdb530fe3442cbe8c6dbfb7ef23365450d5070 AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# The gateway origin the BROWSER will call. Must be declared here, in the stage that runs
# `next build`: NEXT_PUBLIC_* values are inlined into the client bundle at build time and frozen,
# so setting this on the runner stage would have no effect. Previously this was only a comment on
# the runner stage, so compose's build arg was silently discarded and the app fell back to a
# hardcoded (and wrong) origin.
ARG NEXT_PUBLIC_API_BASE
ENV NEXT_PUBLIC_API_BASE=$NEXT_PUBLIC_API_BASE
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:26-alpine@sha256:dbaa92e5758cbbcf85d65d5403fdb530fe3442cbe8c6dbfb7ef23365450d5070 AS runner
LABEL org.opencontainers.image.title="omniflow-frontend" \
      org.opencontainers.image.source="https://github.com/Morpheus7700/omniflow" \
      org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NEXT_TELEMETRY_DISABLED=1
# No NEXT_PUBLIC_* here on purpose — it is already baked into the bundle by the builder stage above.
# Repointing at a different gateway requires a rebuild, not a restart with new env.
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001
COPY --from=builder --chown=nextjs:nodejs /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
USER nextjs
EXPOSE 3000
# Alpine ships wget (busybox), which is enough to probe the app's own listener without adding curl.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=6 \
  CMD ["wget", "-qO", "/dev/null", "http://127.0.0.1:3000/"]
CMD ["node", "server.js"]
