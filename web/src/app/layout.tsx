// layout.tsx
// Layout raiz do aplicativo Next.js.
// Define metadados globais e estrutura HTML base.

import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'SISTEMA DE VOTAÇÃO',
  description: 'Painel de acompanhamento do sistema de votação',
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
