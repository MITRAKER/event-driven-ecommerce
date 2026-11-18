# One Dockerfile for every service: build the whole monorepo once, then pick a service.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages packages
COPY services services
RUN npm ci && npm run build && npm prune --omit=dev

FROM node:22-alpine
ARG SERVICE
ENV NODE_ENV=production SERVICE_NAME=${SERVICE}
WORKDIR /app
COPY --from=build /app /app
WORKDIR /app/services/${SERVICE}
USER node
CMD ["npm", "start"]
