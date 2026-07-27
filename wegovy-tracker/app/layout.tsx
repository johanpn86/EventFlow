import type { Metadata, Viewport } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Wegovy Tracker",
  description: "Privat 4-måneders Wegovy progressions- og vanetracker",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  themeColor: "#0b0f14",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="da" className="dark">
      <body className="min-h-screen bg-surface font-sans antialiased">{children}</body>
    </html>
  );
}
