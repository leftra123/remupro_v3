"use client";

import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AnimatedNumber } from "@/components/motion/animated-number";
import { formatCLP } from "@/lib/utils";

interface BRPDistributionChartProps {
  sep: number;
  pie: number;
  normal: number;
  className?: string;
}

const COLORS = {
  SEP: "#3b82f6",
  PIE: "#10b981",
  Normal: "#f59e0b",
};

export function BRPDistributionChart({ sep, pie, normal, className }: BRPDistributionChartProps) {
  const data = [
    { name: "SEP", value: sep, color: COLORS.SEP },
    { name: "PIE", value: pie, color: COLORS.PIE },
    { name: "Normal", value: normal, color: COLORS.Normal },
  ];

  const total = sep + pie + normal;

  const renderCustomLabel = ({
    cx,
    cy,
    midAngle,
    innerRadius,
    outerRadius,
    percent,
  }: {
    cx: number;
    cy: number;
    midAngle: number;
    innerRadius: number;
    outerRadius: number;
    percent: number;
  }) => {
    const RADIAN = Math.PI / 180;
    const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);

    if (percent < 0.05) return null;

    return (
      <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" className="text-xs font-bold">
        {`${(percent * 100).toFixed(0)}%`}
      </text>
    );
  };

  const CustomTooltip = ({ active, payload }: { active?: boolean; payload?: Array<{ name: string; value: number; payload: { color: string } }> }) => {
    if (active && payload && payload.length) {
      const item = payload[0];
      const pct = total > 0 ? ((item.value / total) * 100).toFixed(1) : "0";
      return (
        <div className="glass rounded-xl p-3 shadow-lg border">
          <div className="flex items-center gap-2">
            <div className="h-3 w-3 rounded-full" style={{ backgroundColor: item.payload.color }} />
            <span className="font-medium">{item.name}</span>
          </div>
          <p className="mt-1 text-sm font-bold">{formatCLP(item.value)}</p>
          <p className="text-xs text-muted-foreground">{pct}% del total</p>
        </div>
      );
    }
    return null;
  };

  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Distribucion BRP por Subvencion</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <defs>
                <linearGradient id="sepGrad" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stopColor="#3b82f6" />
                  <stop offset="100%" stopColor="#60a5fa" />
                </linearGradient>
                <linearGradient id="pieGrad" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stopColor="#10b981" />
                  <stop offset="100%" stopColor="#34d399" />
                </linearGradient>
                <linearGradient id="normalGrad" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stopColor="#f59e0b" />
                  <stop offset="100%" stopColor="#fbbf24" />
                </linearGradient>
              </defs>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={100}
                paddingAngle={3}
                dataKey="value"
                labelLine={false}
                label={renderCustomLabel}
                animationDuration={1200}
                animationBegin={200}
              >
                {data.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={["url(#sepGrad)", "url(#pieGrad)", "url(#normalGrad)"][index]}
                    stroke="transparent"
                  />
                ))}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
              <Legend
                formatter={(value: string) => <span className="text-sm">{value}</span>}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-2 text-center">
          <p className="text-xs text-muted-foreground">Total BRP</p>
          <p className="text-lg font-bold">
            <AnimatedNumber value={total} format={(n) => formatCLP(n)} />
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
