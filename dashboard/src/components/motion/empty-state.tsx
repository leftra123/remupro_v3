"use client";

import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import type { LucideIcon } from "lucide-react";

interface EmptyStateProps {
  icon: LucideIcon;
  title: string;
  description: string;
}

export function EmptyState({ icon: Icon, title, description }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center min-h-[50vh] space-y-6">
      <motion.div
        animate={{
          y: [0, -8, 0],
        }}
        transition={{
          duration: 3,
          repeat: Infinity,
          ease: "easeInOut",
        }}
      >
        <div className="rounded-full bg-muted/50 p-6">
          <Icon className="h-12 w-12 text-muted-foreground" />
        </div>
      </motion.div>
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2, duration: 0.4 }}
        className="text-center space-y-2"
      >
        <h2 className="text-xl font-semibold">{title}</h2>
        <p className="text-muted-foreground text-sm max-w-md">{description}</p>
      </motion.div>
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4, duration: 0.4 }}
        className="flex gap-3"
      >
        <Link href="/upload">
          <Button>Subir Archivos</Button>
        </Link>
      </motion.div>
    </div>
  );
}
