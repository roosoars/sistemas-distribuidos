FROM node:20-alpine AS builder

WORKDIR /app

ARG NEXT_PUBLIC_NAMESERVER_URL=http://localhost:9080
ENV NEXT_PUBLIC_NAMESERVER_URL=${NEXT_PUBLIC_NAMESERVER_URL}

COPY package.json ./
RUN npm install

COPY . .
RUN npm run build

FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 3000

CMD ["node", "server.js"]
