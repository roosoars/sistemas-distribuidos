/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  env: {
    NEXT_PUBLIC_NATS_WS_URL: process.env.NEXT_PUBLIC_NATS_WS_URL || 'ws://localhost:9222',
    NEXT_PUBLIC_NATS_USER: process.env.NEXT_PUBLIC_NATS_USER || 'app_web',
    NEXT_PUBLIC_NATS_PASSWORD: process.env.NEXT_PUBLIC_NATS_PASSWORD || 'app_web_dev',
  },
}

module.exports = nextConfig
