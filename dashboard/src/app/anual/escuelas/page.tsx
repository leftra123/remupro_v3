"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Building2 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PageTransition } from "@/components/motion/page-transition";
import {
  getAnualYears,
  getAnualSchools,
  AnualSchoolEntry,
} from "@/lib/api";
import { formatCLP } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

export default function EscuelasAnualPage() {
  return (
    <Suspense fallback={<div className="p-8 text-center text-muted-foreground">Cargando...</div>}>
      <EscuelasAnualContent />
    </Suspense>
  );
}

function EscuelasAnualContent() {
  const searchParams = useSearchParams();
  const anioParam = searchParams.get("anio");

  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number>(
    anioParam ? parseInt(anioParam) : new Date().getFullYear()
  );
  const [schools, setSchools] = useState<AnualSchoolEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    getAnualYears().then((y) => { if (!cancelled) setYears(y); }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!selectedYear) return;
    let cancelled = false;
    setLoading(true);
    getAnualSchools(selectedYear)
      .then((s) => { if (!cancelled) setSchools(s); })
      .catch(() => { if (!cancelled) setSchools([]); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedYear]);

  const chartData = schools
    .filter((s) => s.rbd && s.rbd !== "DEM")
    .map((s) => ({
      name: s.escuela.length > 20 ? s.escuela.substring(0, 20) + "..." : s.escuela,
      brp: s.brp_total,
      escuela: s.escuela,
    }))
    .sort((a, b) => b.brp - a.brp);

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Escuelas - Liquidacion Anual</h1>
            <p className="text-muted-foreground text-sm">
              Comparacion de BRP entre establecimientos
            </p>
          </div>
          <Select
            value={selectedYear.toString()}
            onValueChange={(v) => setSelectedYear(parseInt(v))}
          >
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Ano" />
            </SelectTrigger>
            <SelectContent>
              {years.map((y) => (
                <SelectItem key={y} value={y.toString()}>
                  {y}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Bar chart */}
        {chartData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">BRP por Establecimiento - {selectedYear}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData} layout="vertical" margin={{ left: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                    <XAxis
                      type="number"
                      tickFormatter={(v: number) => formatCLP(v)}
                      className="text-xs"
                    />
                    <YAxis
                      type="category"
                      dataKey="name"
                      width={160}
                      className="text-xs"
                    />
                    <Tooltip
                      formatter={(value: number) => formatCLP(value)}
                      labelFormatter={(label: string) => label}
                    />
                    <Bar dataKey="brp" name="BRP Total" fill="hsl(221, 83%, 53%)" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Table */}
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Detalle por Establecimiento</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <p className="text-center text-muted-foreground py-8">Cargando...</p>
            ) : schools.length === 0 ? (
              <div className="text-center py-8">
                <Building2 className="h-10 w-10 mx-auto text-muted-foreground/50 mb-3" />
                <p className="text-muted-foreground">Sin datos de escuelas para {selectedYear}</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-xs text-muted-foreground">
                      <th className="text-left py-2 pr-3">Establecimiento</th>
                      <th className="text-left py-2 pr-3">RBD</th>
                      <th className="text-right py-2 pr-3">Docentes</th>
                      <th className="text-right py-2 pr-3">BRP Total</th>
                      <th className="text-right py-2">Haberes Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schools.map((s) => (
                      <tr key={s.rbd} className="border-b last:border-0 hover:bg-accent/50">
                        <td className="py-2 pr-3 font-medium">{s.escuela}</td>
                        <td className="py-2 pr-3 text-muted-foreground">{s.rbd}</td>
                        <td className="py-2 pr-3 text-right">{s.docentes}</td>
                        <td className="py-2 pr-3 text-right font-medium">{formatCLP(s.brp_total)}</td>
                        <td className="py-2 text-right">{formatCLP(s.haberes_total)}</td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="border-t font-bold">
                      <td className="py-2 pr-3">Total</td>
                      <td className="py-2 pr-3"></td>
                      <td className="py-2 pr-3 text-right">
                        {schools.reduce((s, e) => s + e.docentes, 0)}
                      </td>
                      <td className="py-2 pr-3 text-right">
                        {formatCLP(schools.reduce((s, e) => s + e.brp_total, 0))}
                      </td>
                      <td className="py-2 text-right">
                        {formatCLP(schools.reduce((s, e) => s + e.haberes_total, 0))}
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </PageTransition>
  );
}
