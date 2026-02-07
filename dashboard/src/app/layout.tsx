import type { Metadata } from "next";
import "./globals.css";
import { ThemeProvider } from "@/components/theme-provider";
import { AppProvider } from "@/lib/store";
import { ClientLayout } from "./client-layout";

export const metadata: Metadata = {
  title: "RemuPro - Dashboard BRP",
  description: "Sistema de distribucion de Bonificacion de Reconocimiento Profesional",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="es" suppressHydrationWarning>
      <body>
        <ThemeProvider
          attribute="class"
          defaultTheme="dark"
          enableSystem
          disableTransitionOnChange
        >
          <AppProvider>
            <ClientLayout>{children}</ClientLayout>
          </AppProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
