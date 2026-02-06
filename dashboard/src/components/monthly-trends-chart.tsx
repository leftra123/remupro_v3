"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatCLP } from "@/lib/utils";
import type { TrendDataPoint } from "@/lib/api";

interface MonthlyTrendsChartProps {
  data: TrendDataPoint[];
  className?: string;
}

interface TooltipPayloadItem {
  name: string;
  value: number;
  color: string;
  dataKey: string;
}

const AREA_CONFIG = [
  {
    dataKey: "brp_sep",
    label: "SEP",
    stroke: "#3b82f6",
    fill: "#60a5fa",
    gradientId: "gradSep",
    stopColor: "#3b82f6",
    stopColorEnd: "#60a5fa",
  },
  {
    dataKey: "brp_pie",
    label: "PIE",
    stroke: "#10b981",
    fill: "#34d399",
    gradientId: "gradPie",
    stopColor: "#10b981",
    stopColorEnd: "#34d399",
  },
  {
    dataKey: "brp_normal",
    label: "Normal",
    stroke: "#f59e0b",
    fill: "#fbbf24",
    gradientId: "gradNormal",
    stopColor: "#f59e0b",
    stopColorEnd: "#fbbf24",
  },
] as const;

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: TooltipPayloadItem[];
  label?: string;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const total = payload.reduce((sum, item) => sum + item.value, 0);

  return (
    <div className="rounded-xl border bg-background p-3 shadow-lg">
      <p className="mb-2 font-semibold text-foreground">{label}</p>
      {payload.map((item) => (
        <div key={item.dataKey} className="flex items-center justify-between gap-4 text-sm">
          <div className="flex items-center gap-2">
            <div
              className="h-3 w-3 rounded-full"
              style={{ backgroundColor: item.color }}
            />
            <span className="text-muted-foreground">{item.name}</span>
          </div>
          <span className="font-medium text-foreground">{formatCLP(item.value)}</span>
        </div>
      ))}
      <div className="mt-2 border-t pt-2">
        <div className="flex items-center justify-between text-sm font-bold">
          <span>Total</span>
          <span>{formatCLP(total)}</span>
        </div>
      </div>
    </div>
  );
}

function formatYAxis(value: number): string {
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value}`;
}

export function MonthlyTrendsChart({ data, className }: MonthlyTrendsChartProps) {
  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Tendencias Mensuales</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={data}
              margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
            >
              <defs>
                {AREA_CONFIG.map((area) => (
                  <linearGradient
                    key={area.gradientId}
                    id={area.gradientId}
                    x1="0"
                    y1="0"
                    x2="0"
                    y2="1"
                  >
                    <stop offset="5%" stopColor={area.stopColor} stopOpacity={0.4} />
                    <stop offset="95%" stopColor={area.stopColorEnd} stopOpacity={0.05} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid
                strokeDasharray="3 3"
                className="stroke-muted"
                vertical={false}
              />
              <XAxis
                dataKey="mes"
                tick={{ fontSize: 12 }}
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                tickFormatter={formatYAxis}
                tick={{ fontSize: 12 }}
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
                width={60}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                formatter={(value: string) => (
                  <span className="text-sm text-foreground">{value}</span>
                )}
              />
              {AREA_CONFIG.map((area) => (
                <Area
                  key={area.dataKey}
                  type="monotone"
                  dataKey={area.dataKey}
                  name={area.label}
                  stackId="brp"
                  stroke={area.stroke}
                  fill={`url(#${area.gradientId})`}
                  strokeWidth={2}
                  animationDuration={1200}
                  animationBegin={200}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
