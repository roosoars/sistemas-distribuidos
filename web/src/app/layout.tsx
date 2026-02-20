// layout.tsx
// Layout raiz do aplicativo Next.js.
// Define metadados globais e estrutura HTML base.

import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Sistema de Votacao Distribuido',
  description: 'Dashboard de monitoramento do sistema de votacao com Leader Election',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  )
}
