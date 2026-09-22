import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";

// Three roles, each chosen for institutional provenance rather than neutrality:
// Newsreader carries editorial authority for the few statements that matter; Public Sans is the US
// Web Design System's face, which reads as civic record-keeping; IBM Plex Mono sets every figure,
// because HLC keys and currency have to align digit-for-digit to be scannable.
//
// LOCAL, not `next/font/google`. The loader that fetches from Google's CDN does so at BUILD time,
// which made every image build depend on reaching fonts.gstatic.com — and that dependency failed a
// CI boot job (`Module not found: Can't resolve '@vercel/turbopack-next/internal/font/google/font'`,
// one error per face) while the same `npm run build` had just succeeded on the runner minutes
// earlier. A build that can fail for a reason outside the repository is not reproducible, and an
// air-gapped build could never have worked at all. The files here are the latin subset Google
// serves, vendored; Newsreader and Public Sans are variable fonts, so one file covers 400–500.
// All three are SIL Open Font License 1.1 — see fonts/OFL.txt.
const display = localFont({
  variable: "--font-display",
  display: "swap",
  src: [
    { path: "./fonts/newsreader-variable.woff2", weight: "400 500", style: "normal" },
    { path: "./fonts/newsreader-variable-italic.woff2", weight: "400 500", style: "italic" },
  ],
});

const body = localFont({
  variable: "--font-body",
  display: "swap",
  src: [{ path: "./fonts/public-sans-variable.woff2", weight: "400 500", style: "normal" }],
});

const data = localFont({
  variable: "--font-data",
  display: "swap",
  src: [
    { path: "./fonts/ibm-plex-mono-400.woff2", weight: "400", style: "normal" },
    { path: "./fonts/ibm-plex-mono-500.woff2", weight: "500", style: "normal" },
  ],
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
