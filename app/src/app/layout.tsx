import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Knoggin",
  description: "Source-grounded memory for AI agents and personal tools.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
