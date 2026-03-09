FROM node:20-alpine AS builder

WORKDIR /app

ARG NEXT_PUBLIC_NATS_WS_URL=ws://localhost:9222
ENV NEXT_PUBLIC_NATS_WS_URL=${NEXT_PUBLIC_NATS_WS_URL}
ARG NEXT_PUBLIC_NATS_USER=app_web
ENV NEXT_PUBLIC_NATS_USER=${NEXT_PUBLIC_NATS_USER}
ARG NEXT_PUBLIC_NATS_PASSWORD=app_web_dev
ENV NEXT_PUBLIC_NATS_PASSWORD=${NEXT_PUBLIC_NATS_PASSWORD}

COPY package.json ./
RUN npm install

COPY . .
RUN npm run build

FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000

CMD ["node", "server.js"]
