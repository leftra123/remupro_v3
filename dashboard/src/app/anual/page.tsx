"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  CalendarDays,
  Users,
  Building2,
  DollarSign,
  TrendingUp,
  Search,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { PageTransition } from "@/components/motion/page-transition";
import {
  getAnualYears,
  getAnualSummary,
  getAnualTrends,
  AnualSummary,
  AnualTrendPoint,
} from "@/lib/api";
import { formatCLP } from "@/lib/utils";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

export default function AnualPage() {
  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);
  const [summary, setSummary] = useState<AnualSummary | null>(null);
  const [trends, setTrends] = useState<AnualTrendPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getAnualYears()
      .then((y) => {
        setYears(y);
        if (y.length > 0) setSelectedYear(y[0]);
      })
      .catch(() => setYears([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selectedYear) return;
    setLoading(true);
    Promise.all([getAnualSummary(selectedYear), getAnualTrends(selectedYear)])
      .then(([s, t]) => {
        setSummary(s);
        setTrends(t);
      })
      .catch(() => {
        setSummary(null);
        setTrends([]);
      })
      .finally(() => setLoading(false));
  }, [selectedYear]);

  if (loading && years.length === 0) {
    return (
      <PageTransition>
        <div className="flex items-center justify-center h-64 text-muted-foreground">
          Cargando datos anuales...
        </div>
      </PageTransition>
    );
  }

  if (years.length === 0) {
    return (
      <PageTransition>
        <div className="space-y-6 max-w-4xl">
          <h1 className="text-2xl font-bold">Liquidacion Anual</h1>
          <Card>
            <CardContent className="py-12 text-center">
              <CalendarDays className="h-12 w-12 mx-auto text-muted-foreground/50 mb-4" />
              <p className="text-lg font-medium mb-2">Sin datos anuales</p>
              <p className="text-sm text-muted-foreground mb-4">
                Suba un archivo anual consolidado desde la pagina de Upload para comenzar.
              </p>
              <Link href="/upload">
                <Button>Subir Archivo Anual</Button>
              </Link>
            </CardContent>
          </Card>
        </div>
      </PageTransition>
    );
  }

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Liquidacion Anual</h1>
            <p className="text-muted-foreground text-sm">
              Resumen consolidado de liquidaciones por ano
            </p>
          </div>
          <Select
            value={selectedYear?.toString() ?? ""}
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

        {summary && (
          <>
            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                    <DollarSign className="h-4 w-4" />
                    Total BRP
                  </div>
                  <p className="text-xl font-bold">{formatCLP(summary.brp_total_anual)}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                    <Users className="h-4 w-4" />
                    Docentes
                  </div>
                  <p className="text-xl font-bold">{summary.total_docentes}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                    <Building2 className="h-4 w-4" />
                    Establecimientos
                  </div>
                  <p className="text-xl font-bold">{summary.total_establecimientos}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                    <TrendingUp className="h-4 w-4" />
                    Total Haberes
                  </div>
                  <p className="text-xl font-bold">{formatCLP(summary.haberes_total_anual)}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                    <DollarSign className="h-4 w-4" />
                    Liquido Total
                  </div>
                  <p className="text-xl font-bold">{formatCLP(summary.liquido_total_anual)}</p>
                </CardContent>
              </Card>
            </div>

            {/* Trends chart */}
            {trends.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Tendencia BRP Mensual - {selectedYear}</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={trends}>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis
                          dataKey="mes"
                          className="text-xs"
                          tickFormatter={(v: string) => v.split("-")[1] || v}
                        />
                        <YAxis
                          className="text-xs"
                          tickFormatter={(v: number) => formatCLP(v)}
                        />
                        <Tooltip
                          formatter={(value: number) => formatCLP(value)}
                          labelFormatter={(label: string) => `Mes: ${label}`}
                        />
                        <Legend />
                        <Bar dataKey="brp_sep" name="SEP" stackId="brp" fill="hsl(221, 83%, 53%)" />
                        <Bar dataKey="brp_pie" name="PIE" stackId="brp" fill="hsl(160, 84%, 39%)" />
                        <Bar dataKey="brp_normal" name="Normal" stackId="brp" fill="hsl(38, 92%, 50%)" />
                        <Bar dataKey="brp_eib" name="EIB" stackId="brp" fill="hsl(280, 67%, 51%)" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Quick links */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Link href={`/anual/docentes?anio=${selectedYear}`}>
                <Card className="hover:bg-accent/50 transition-colors cursor-pointer">
                  <CardContent className="pt-4 pb-3 flex items-center gap-3">
                    <Search className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Buscar Docentes</p>
                      <p className="text-xs text-muted-foreground">Busqueda por RUT o nombre</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
              <Link href={`/anual/escuelas?anio=${selectedYear}`}>
                <Card className="hover:bg-accent/50 transition-colors cursor-pointer">
                  <CardContent className="pt-4 pb-3 flex items-center gap-3">
                    <Building2 className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Escuelas</p>
                      <p className="text-xs text-muted-foreground">Comparacion entre establecimientos</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
              <Link href={`/anual/multi-establecimiento?anio=${selectedYear}`}>
                <Card className="hover:bg-accent/50 transition-colors cursor-pointer">
                  <CardContent className="pt-4 pb-3 flex items-center gap-3">
                    <Users className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium">Multi-Establecimiento</p>
                      <p className="text-xs text-muted-foreground">Docentes en 2+ escuelas</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            </div>
          </>
        )}
      </div>
    </PageTransition>
  );
}
