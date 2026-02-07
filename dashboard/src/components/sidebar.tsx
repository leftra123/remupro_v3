"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "framer-motion";
import {
  LayoutDashboard,
  Upload,
  Table2,
  Building2,
  ClipboardList,
  Bell,
  BarChart3,
  CalendarDays,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { cn, formatCLP } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { ThemeToggle } from "@/components/theme-toggle";
import { useAppState } from "@/lib/store";
import { Badge } from "@/components/ui/badge";
import { AnimatedNumber } from "@/components/motion/animated-number";
import { useState } from "react";

const NAV_ITEMS = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/upload", label: "Subir Archivos", icon: Upload },
  { href: "/results", label: "Resultados", icon: Table2 },
  { href: "/multi-establecimiento", label: "Distribucion por Escuela", icon: Building2 },
  { href: "/auditoria", label: "Auditoria", icon: ClipboardList },
  { href: "/alertas", label: "Alertas", icon: Bell },
  { href: "/anual", label: "Liquidacion Anual", icon: CalendarDays },
];

export function Sidebar() {
  const pathname = usePathname();
  const { summary, sessionId, auditLog, selectedMonth } = useAppState();
  const [collapsed, setCollapsed] = useState(false);

  const warningCount = auditLog.filter((e) => e.nivel === "WARNING").length;
  const errorCount = auditLog.filter((e) => e.nivel === "ERROR").length;

  return (
    <aside
      className={cn(
        "flex flex-col border-r bg-card transition-all duration-300",
        collapsed ? "w-16" : "w-64"
      )}
    >
      {/* Header with gradient */}
      <div className="flex h-16 items-center justify-between px-4 border-b bg-gradient-to-r from-primary/5 to-transparent">
        {!collapsed && (
          <Link href="/" className="flex items-center gap-2">
            <BarChart3 className="h-6 w-6 text-primary" />
            <span className="font-bold text-lg">RemuPro</span>
          </Link>
        )}
        {collapsed && (
          <Link href="/" className="mx-auto">
            <BarChart3 className="h-6 w-6 text-primary" />
          </Link>
        )}
        <Button
          variant="ghost"
          size="icon"
          className={cn("h-8 w-8", collapsed && "mx-auto")}
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
        </Button>
      </div>

      {/* Status */}
      {!collapsed && (
        <div className="px-4 py-3 space-y-2">
          <Badge variant={sessionId ? "default" : "outline"} className="w-full justify-center">
            {sessionId ? "Datos Cargados" : "Sin datos"}
          </Badge>
          {selectedMonth && (
            <div className="text-xs text-center text-muted-foreground">
              Mes: <span className="font-medium text-foreground">{selectedMonth}</span>
            </div>
          )}
        </div>
      )}

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-2 py-2">
        {NAV_ITEMS.map((item) => {
          const isActive = pathname === item.href || (item.href !== '/' && pathname.startsWith(item.href));
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                isActive
                  ? "text-primary-foreground"
                  : "text-muted-foreground hover:text-accent-foreground",
                collapsed && "justify-center px-2"
              )}
              title={collapsed ? item.label : undefined}
            >
              {isActive && (
                <motion.div
                  layoutId="activeNav"
                  className="absolute inset-0 bg-primary rounded-lg"
                  transition={{ type: "spring", stiffness: 350, damping: 30 }}
                />
              )}
              {!isActive && (
                <motion.div
                  className="absolute inset-0 rounded-lg"
                  whileHover={{ backgroundColor: "hsl(var(--accent))" }}
                  transition={{ duration: 0.15 }}
                />
              )}
              <motion.div
                className="relative z-10 flex items-center gap-3"
                whileHover={!isActive ? { x: 4 } : undefined}
                transition={{ duration: 0.15 }}
              >
                <item.icon className="h-5 w-5 shrink-0" />
                {!collapsed && <span>{item.label}</span>}
              </motion.div>
              {!collapsed && item.href === "/auditoria" && (warningCount + errorCount) > 0 && (
                <Badge variant="destructive" className="relative z-10 ml-auto h-5 min-w-[20px] justify-center px-1 text-[10px]">
                  {warningCount + errorCount}
                </Badge>
              )}
            </Link>
          );
        })}
      </nav>

      {/* Mini Stats */}
      {!collapsed && summary && (
        <>
          <Separator />
          <div className="p-4 space-y-3">
            <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
              Resumen Rapido
            </p>
            <div className="space-y-2">
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Total BRP</span>
                <span className="font-medium">
                  <AnimatedNumber value={summary.total_brp} format={(n) => formatCLP(n)} />
                </span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-blue-500">SEP</span>
                <span className="font-medium">
                  <AnimatedNumber value={summary.total_sep} format={(n) => formatCLP(n)} />
                </span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-emerald-500">PIE</span>
                <span className="font-medium">
                  <AnimatedNumber value={summary.total_pie} format={(n) => formatCLP(n)} />
                </span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-amber-500">Normal</span>
                <span className="font-medium">
                  <AnimatedNumber value={summary.total_normal} format={(n) => formatCLP(n)} />
                </span>
              </div>
              <Separator className="my-1" />
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Docentes</span>
                <span className="font-medium">{summary.total_docentes}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Establec.</span>
                <span className="font-medium">{summary.total_establecimientos}</span>
              </div>
            </div>
          </div>
        </>
      )}

      {/* Footer */}
      <Separator />
      <div className="flex items-center justify-between p-3">
        <ThemeToggle />
        {!collapsed && (
          <span className="text-xs text-muted-foreground">v3.0</span>
        )}
      </div>
    </aside>
  );
}
