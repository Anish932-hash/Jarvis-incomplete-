import type {Metadata} from 'next';
import { Orbitron } from 'next/font/google';
import './globals.css';
import { Toaster } from "@/components/ui/toaster"

const orbitron = Orbitron({
  subsets: ['latin'],
  weight: ['400', '500', '700', '900'],
  display: 'swap',
});

export const metadata: Metadata = {
  title: 'Neon OS',
  description: 'A futuristic HUD interface for system monitoring.',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${orbitron.className} font-body antialiased`}>
        {children}
        <Toaster />
      </body>
    </html>
  );
}
