# One Dockerfile for every service: build the whole monorepo once, then pick a service.
#   docker build --build-arg SERVICE=order-service -t order-service .

# ---- Stage 1: build (has the TypeScript compiler and dev dependencies; thrown away at the end)
FROM node:22-alpine AS build
WORKDIR /app

# Copy only the package manifests first. Docker caches each step, so as long as no
# package.json changes, the slow `npm ci` below is reused when you only edit source code.
COPY package.json package-lock.json ./
COPY packages/contracts/package.json packages/contracts/
COPY packages/messaging/package.json packages/messaging/
COPY services/order-service/package.json services/order-service/
COPY services/inventory-service/package.json services/inventory-service/
COPY services/payment-service/package.json services/payment-service/
COPY services/order-query-service/package.json services/order-query-service/

# --ignore-scripts: no dependency gets to run its install scripts inside the build (a common
# supply-chain attack vector), and husky's "prepare" git-hook setup is skipped.
RUN npm ci --ignore-scripts

COPY tsconfig.base.json ./
COPY packages packages
COPY services services

# Compile, drop the dev dependencies, then delete the TypeScript sources and tsconfig files.
# What is left is what the runtime needs: compiled JS, SQL migrations, package.json files and
# production node_modules. The globs only match the top of each workspace, never anything
# inside node_modules. The workspace symlinks (node_modules/@shop/* -> packages/*) keep working
# because their targets keep package.json and dist/.
RUN npm run build \
  && npm prune --omit=dev --ignore-scripts \
  && rm -rf tsconfig.base.json \
    packages/*/src packages/*/tsconfig*.json \
    services/*/src services/*/tsconfig*.json

# ---- Stage 2: runtime (small image: no compiler, no test tools, no source code)
FROM node:22-alpine
ARG SERVICE
ENV NODE_ENV=production SERVICE_NAME=${SERVICE}
# Files stay owned by root and the process runs as `node`, so the app cannot modify its own code.
COPY --from=build /app /app
# The service finds its SQL migrations relative to this directory.
WORKDIR /app/services/${SERVICE}
USER node
# Run node directly, not `npm start`: Docker sends SIGTERM to the main process, and npm does not
# reliably pass it on, so the graceful shutdown would never run. PORT, DATABASE_URL and
# KAFKA_BROKERS come from docker-compose.yml (or your orchestrator).
CMD ["node", "dist/main.js"]
