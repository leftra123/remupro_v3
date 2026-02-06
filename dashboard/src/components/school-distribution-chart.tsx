"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatCLP } from "@/lib/utils";
import type { SchoolSummary } from "@/lib/api";

interface SchoolDistributionChartProps {
  data: SchoolSummary[];
  className?: string;
}

export function SchoolDistributionChart({ data, className }: SchoolDistributionChartProps) {
  const chartData = data.map((s) => ({
    escuela: s.escuela.length > 20 ? s.escuela.substring(0, 20) + "..." : s.escuela,
    SEP: s.brp_sep,
    PIE: s.brp_pie,
    Normal: s.brp_normal,
  }));

  const formatYAxis = (value: number) => {
    if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
    if (value >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
    return `$${value}`;
  };

  const CustomTooltip = ({ active, payload, label }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string }>; label?: string }) => {
    if (active && payload && payload.length) {
      const total = payload.reduce((sum, p) => sum + p.value, 0);
      return (
        <div className="glass rounded-xl p-3 shadow-lg border min-w-[200px]">
          <p className="font-medium text-sm mb-2">{label}</p>
          {payload.map((p, i) => (
            <div key={i} className="flex items-center justify-between gap-4 text-xs">
              <div className="flex items-center gap-1.5">
                <div className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: p.color }} />
                <span>{p.name}</span>
              </div>
              <span className="font-medium">{formatCLP(p.value)}</span>
            </div>
          ))}
          <div className="mt-1.5 pt-1.5 border-t flex justify-between text-xs font-bold">
            <span>Total</span>
            <span>{formatCLP(total)}</span>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Distribucion por Establecimiento</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ bottom: 60 }}>
              <defs>
                <linearGradient id="schoolSepGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3b82f6" />
                  <stop offset="100%" stopColor="#60a5fa" />
                </linearGradient>
                <linearGradient id="schoolPieGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#10b981" />
                  <stop offset="100%" stopColor="#34d399" />
                </linearGradient>
                <linearGradient id="schoolNormalGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#f59e0b" />
                  <stop offset="100%" stopColor="#fbbf24" />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                dataKey="escuela"
                className="text-xs"
                angle={-35}
                textAnchor="end"
                interval={0}
                height={80}
              />
              <YAxis tickFormatter={formatYAxis} className="text-xs" />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Bar dataKey="SEP" stackId="a" fill="url(#schoolSepGrad)" radius={[0, 0, 0, 0]} animationDuration={1200} />
              <Bar dataKey="PIE" stackId="a" fill="url(#schoolPieGrad)" radius={[0, 0, 0, 0]} animationDuration={1200} animationBegin={200} />
              <Bar dataKey="Normal" stackId="a" fill="url(#schoolNormalGrad)" radius={[4, 4, 0, 0]} animationDuration={1200} animationBegin={400} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
