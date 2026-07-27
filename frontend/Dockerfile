FROM node:26-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:26-alpine AS builder
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
RUN npm run build

FROM node:26-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
# No NEXT_PUBLIC_* here on purpose — it is already baked into the bundle by the builder stage above.
# Repointing at a different gateway requires a rebuild, not a restart with new env.
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
USER nextjs
EXPOSE 3000
CMD ["node", "server.js"]
