import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
  {
    variants: {
      variant: {
        default: "border-transparent bg-primary text-primary-foreground hover:bg-primary/80",
        secondary: "border-transparent bg-secondary text-secondary-foreground hover:bg-secondary/80",
        destructive: "border-transparent bg-destructive text-destructive-foreground hover:bg-destructive/80",
        outline: "text-foreground",
        sep: "border-transparent bg-blue-500/15 text-blue-600 dark:text-blue-400",
        pie: "border-transparent bg-emerald-500/15 text-emerald-600 dark:text-emerald-400",
        normal: "border-transparent bg-amber-500/15 text-amber-600 dark:text-amber-400",
        daem: "border-transparent bg-indigo-500/15 text-indigo-600 dark:text-indigo-400",
        cpeip: "border-transparent bg-pink-500/15 text-pink-600 dark:text-pink-400",
        info: "border-transparent bg-blue-500/15 text-blue-600 dark:text-blue-400",
        warning: "border-transparent bg-amber-500/15 text-amber-600 dark:text-amber-400",
        error: "border-transparent bg-red-500/15 text-red-600 dark:text-red-400",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };
