"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/utils";
import { AnimatedNumber } from "@/components/motion/animated-number";
import type { LucideIcon } from "lucide-react";

interface StatsCardProps {
  title: string;
  value: string;
  subtitle?: string;
  icon: LucideIcon;
  color?: string;
  className?: string;
  numericValue?: number;
  formatFn?: (n: number) => string;
}

const GRADIENT_MAP: Record<string, string> = {
  "text-primary": "from-blue-500/20 to-indigo-500/20",
  "text-emerald-500": "from-emerald-500/20 to-teal-500/20",
  "text-indigo-500": "from-indigo-500/20 to-purple-500/20",
  "text-pink-500": "from-pink-500/20 to-rose-500/20",
  "text-amber-500": "from-amber-500/20 to-orange-500/20",
  "text-blue-500": "from-blue-500/20 to-cyan-500/20",
};

export function StatsCard({
  title,
  value,
  subtitle,
  icon: Icon,
  color = "text-primary",
  className,
  numericValue,
  formatFn,
}: StatsCardProps) {
  const gradient = GRADIENT_MAP[color] || GRADIENT_MAP["text-primary"];

  return (
    <motion.div
      whileHover={{ y: -2 }}
      transition={{ duration: 0.2 }}
      className={cn(
        "relative overflow-hidden rounded-xl border bg-card text-card-foreground shadow-sm transition-shadow hover:shadow-md",
        className
      )}
    >
      {/* Decorative gradient circle */}
      <div
        className={cn(
          "absolute -top-6 -right-6 h-24 w-24 rounded-full bg-gradient-to-br opacity-50 blur-xl",
          gradient
        )}
      />
      <div className="relative p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <p className="text-sm font-medium text-muted-foreground">{title}</p>
            <p className={cn("text-2xl font-bold tracking-tight", color)}>
              {numericValue !== undefined && formatFn ? (
                <AnimatedNumber value={numericValue} format={formatFn} />
              ) : (
                value
              )}
            </p>
            {subtitle && (
              <p className="text-xs text-muted-foreground">{subtitle}</p>
            )}
          </div>
          <div className={cn("rounded-full p-3 bg-gradient-to-br", gradient)}>
            <Icon className={cn("h-6 w-6", color)} />
          </div>
        </div>
      </div>
    </motion.div>
  );
}
