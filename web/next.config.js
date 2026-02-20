/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  env: {
    NEXT_PUBLIC_API_S1: process.env.NEXT_PUBLIC_API_S1 || 'http://localhost:9001',
    NEXT_PUBLIC_API_S2: process.env.NEXT_PUBLIC_API_S2 || 'http://localhost:9002',
    NEXT_PUBLIC_API_S3: process.env.NEXT_PUBLIC_API_S3 || 'http://localhost:9003',
  },
}

module.exports = nextConfig
