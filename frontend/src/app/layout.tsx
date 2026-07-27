import type { Metadata } from "next";
import { Newsreader, Public_Sans, IBM_Plex_Mono } from "next/font/google";
import "./globals.css";

// Three roles, each chosen for institutional provenance rather than neutrality:
// Newsreader carries editorial authority for the few statements that matter; Public Sans is the US
// Web Design System's face, which reads as civic record-keeping; IBM Plex Mono sets every figure,
// because HLC keys and currency have to align digit-for-digit to be scannable.
const display = Newsreader({
  variable: "--font-display",
  subsets: ["latin"],
  weight: ["400", "500"],
  style: ["normal", "italic"],
});

const body = Public_Sans({
  variable: "--font-body",
  subsets: ["latin"],
});

const data = IBM_Plex_Mono({
  variable: "--font-data",
  subsets: ["latin"],
  weight: ["400", "500"],
});

export const metadata: Metadata = {
  title: "OmniFlow — Procure-to-pay orchestration",
  description:
    "Every purchase order carries a provable position in a single global order. Watch settlement advance in real time, and replay any window exactly as it happened.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={`${display.variable} ${body.variable} ${data.variable} h-full`}
    >
      <body className="min-h-full">{children}</body>
    </html>
  );
}
