"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import {
  DollarSign,
  Users,
  Building2,
  TrendingUp,
  Award,
  Star,
  Zap,
  BarChart3,
  Calendar,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { StatsCard } from "@/components/stats-card";
import { BRPDistributionChart } from "@/components/brp-distribution-chart";
import { DAEMCPEIPChart } from "@/components/daem-cpeip-chart";
import { SchoolDistributionChart } from "@/components/school-distribution-chart";
import { MonthlyTrendsChart } from "@/components/monthly-trends-chart";
import { PageTransition } from "@/components/motion/page-transition";
import { StaggerContainer, StaggerItem } from "@/components/motion/stagger-children";
import { AnimatedNumber } from "@/components/motion/animated-number";
import { useAppState } from "@/lib/store";
import { formatCLP } from "@/lib/utils";
import { getAvailableMonths, getTrends, getMonthSummary } from "@/lib/api";
import Link from "next/link";
import dynamic from "next/dynamic";

const D3Treemap = dynamic(() => import("@/components/charts/d3-treemap").then((m) => m.D3Treemap), {
  ssr: false,
  loading: () => (
    <div className="h-[400px] rounded-xl border bg-card animate-pulse flex items-center justify-center">
      <p className="text-muted-foreground text-sm">Cargando visualizacion...</p>
    </div>
  ),
});

export default function DashboardPage() {
  const {
    summary, schoolSummary, records, sessionId, isDemo, loadDemoData,
    selectedMonth, setSelectedMonth, availableMonths, setAvailableMonths,
    historicalSummary, setHistoricalSummary, trends, setTrends,
  } = useAppState();

  const [loadingHistorical, setLoadingHistorical] = useState(false);

  // Load available months and trends on mount
  useEffect(() => {
    const loadDashboardData = async () => {
      try {
        const [months, trendsData] = await Promise.all([
          getAvailableMonths(),
          getTrends(),
        ]);
        setAvailableMonths(months);
        setTrends(trendsData);
      } catch {
        // API not available, continue with current data
      }
    };
    loadDashboardData();
  }, [setAvailableMonths, setTrends]);

  // Load historical summary when month changes
  useEffect(() => {
    if (!selectedMonth) {
      setHistoricalSummary(null);
      return;
    }
    const loadMonth = async () => {
      setLoadingHistorical(true);
      try {
        const data = await getMonthSummary(selectedMonth);
        setHistoricalSummary(data);
      } catch {
        setHistoricalSummary(null);
      } finally {
        setLoadingHistorical(false);
      }
    };
    loadMonth();
  }, [selectedMonth, setHistoricalSummary]);

  // Determine which summary to display: historical or current session
  const displaySummary = selectedMonth && historicalSummary
    ? {
        total_brp: Number(historicalSummary.brp_total ?? 0),
        total_sep: Number(historicalSummary.brp_sep ?? 0),
        total_pie: Number(historicalSummary.brp_pie ?? 0),
        total_normal: Number(historicalSummary.brp_normal ?? 0),
        total_daem: Math.round(Number(historicalSummary.brp_total ?? 0) * 0.6),
        total_cpeip: Math.round(Number(historicalSummary.brp_total ?? 0) * 0.4),
        total_docentes: Number(historicalSummary.total_docentes ?? 0),
        total_establecimientos: Number(historicalSummary.total_establecimientos ?? 0),
        total_reconocimiento: Number(historicalSummary.reconocimiento_total ?? 0),
        total_tramo: Number(historicalSummary.tramo_total ?? 0),
        total_prioritarios: 0,
      }
    : summary;

  const hasData = (sessionId && summary) || selectedMonth;
  const hasTrends = trends.length > 0;

  if (!hasData && !hasTrends) {
    return (
      <PageTransition>
        <div className="flex flex-col items-center justify-center min-h-[60vh] space-y-6">
          <motion.div
            animate={{ y: [0, -8, 0] }}
            transition={{ duration: 3, repeat: Infinity, ease: "easeInOut" }}
          >
            <div className="rounded-full bg-gradient-to-br from-primary/10 to-primary/5 p-6">
              <BarChart3 className="h-16 w-16 text-primary" />
            </div>
          </motion.div>
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-center space-y-3"
          >
            <h1 className="text-3xl font-bold">RemuPro Dashboard</h1>
            <p className="text-muted-foreground max-w-md">
              Aun no hay datos. Procese archivos para comenzar a ver la distribucion
              de la Bonificacion de Reconocimiento Profesional (BRP).
            </p>
          </motion.div>
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            className="flex gap-3"
          >
            <Link href="/upload">
              <Button size="lg">Subir Archivos</Button>
            </Link>
            <Button variant="outline" size="lg" onClick={loadDemoData}>
              Ver Demo
            </Button>
          </motion.div>
          <StaggerContainer className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-xl w-full mt-8" staggerDelay={0.15}>
            <StaggerItem>
              <Card className="text-center">
                <CardContent className="pt-6">
                  <TrendingUp className="mx-auto h-8 w-8 text-blue-500 mb-2" />
                  <p className="font-medium text-sm">Distribucion BRP</p>
                  <p className="text-xs text-muted-foreground mt-1">SEP, PIE y Normal</p>
                </CardContent>
              </Card>
            </StaggerItem>
            <StaggerItem>
              <Card className="text-center">
                <CardContent className="pt-6">
                  <Building2 className="mx-auto h-8 w-8 text-emerald-500 mb-2" />
                  <p className="font-medium text-sm">Multi-Establecimiento</p>
                  <p className="text-xs text-muted-foreground mt-1">Docentes en 2+ escuelas</p>
                </CardContent>
              </Card>
            </StaggerItem>
          </StaggerContainer>
        </div>
      </PageTransition>
    );
  }

  const avgBRP = displaySummary && displaySummary.total_docentes > 0
    ? displaySummary.total_brp / displaySummary.total_docentes
    : 0;

  return (
    <PageTransition>
      <div className="space-y-6">
        {/* Header with month selector */}
        <div className="flex items-center justify-between flex-wrap gap-4">
          <div>
            <h1 className="text-2xl font-bold">Dashboard</h1>
            <p className="text-muted-foreground text-sm">
              {selectedMonth
                ? `Datos historicos - ${selectedMonth}`
                : "Resumen general de la distribucion BRP"}
            </p>
          </div>
          <div className="flex items-center gap-3">
            {availableMonths.length > 0 && (
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-muted-foreground" />
                <Select
                  value={selectedMonth || "current"}
                  onValueChange={(v) => setSelectedMonth(v === "current" ? null : v)}
                >
                  <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Seleccionar mes" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="current">Sesion actual</SelectItem>
                    {availableMonths.map((m) => (
                      <SelectItem key={m} value={m}>{m}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            {isDemo && (
              <Badge variant="secondary" className="text-sm">
                Modo Demo
              </Badge>
            )}
          </div>
        </div>

        {/* Loading state for historical data */}
        {loadingHistorical && (
          <Card>
            <CardContent className="p-8 text-center">
              <div className="animate-pulse space-y-4">
                <div className="h-4 bg-muted rounded w-1/3 mx-auto" />
                <div className="h-8 bg-muted rounded w-1/2 mx-auto" />
              </div>
            </CardContent>
          </Card>
        )}

        {/* Summary cards */}
        {displaySummary && !loadingHistorical && (
          <>
            {/* Top-level summary cards */}
            <StaggerContainer className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <StaggerItem>
                <StatsCard
                  title="Total BRP"
                  value={formatCLP(displaySummary.total_brp)}
                  numericValue={displaySummary.total_brp}
                  formatFn={(n) => formatCLP(n)}
                  subtitle={`Promedio: ${formatCLP(avgBRP)} / docente`}
                  icon={DollarSign}
                  color="text-primary"
                />
              </StaggerItem>
              <StaggerItem>
                <StatsCard
                  title="Docentes"
                  value={displaySummary.total_docentes.toString()}
                  subtitle={`${displaySummary.total_establecimientos} establecimientos`}
                  icon={Users}
                  color="text-emerald-500"
                />
              </StaggerItem>
              <StaggerItem>
                <StatsCard
                  title="DAEM"
                  value={formatCLP(displaySummary.total_daem)}
                  numericValue={displaySummary.total_daem}
                  formatFn={(n) => formatCLP(n)}
                  subtitle="60% del total"
                  icon={Building2}
                  color="text-indigo-500"
                />
              </StaggerItem>
              <StaggerItem>
                <StatsCard
                  title="CPEIP"
                  value={formatCLP(displaySummary.total_cpeip)}
                  numericValue={displaySummary.total_cpeip}
                  formatFn={(n) => formatCLP(n)}
                  subtitle="40% del total"
                  icon={Star}
                  color="text-pink-500"
                />
              </StaggerItem>
            </StaggerContainer>

            {/* Subsidy cards */}
            <StaggerContainer className="grid grid-cols-1 md:grid-cols-3 gap-4" staggerDelay={0.08}>
              <StaggerItem>
                <Card className="border-l-4 border-l-blue-500">
                  <CardContent className="p-4 flex items-center justify-between">
                    <div>
                      <p className="text-xs text-muted-foreground">BRP SEP</p>
                      <p className="text-xl font-bold text-blue-600 dark:text-blue-400">
                        <AnimatedNumber value={displaySummary.total_sep} format={(n) => formatCLP(n)} />
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {displaySummary.total_brp > 0
                          ? ((displaySummary.total_sep / displaySummary.total_brp) * 100).toFixed(1)
                          : "0"}%
                        del total
                      </p>
                    </div>
                    <Badge variant="sep">SEP</Badge>
                  </CardContent>
                </Card>
              </StaggerItem>
              <StaggerItem>
                <Card className="border-l-4 border-l-emerald-500">
                  <CardContent className="p-4 flex items-center justify-between">
                    <div>
                      <p className="text-xs text-muted-foreground">BRP PIE</p>
                      <p className="text-xl font-bold text-emerald-600 dark:text-emerald-400">
                        <AnimatedNumber value={displaySummary.total_pie} format={(n) => formatCLP(n)} />
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {displaySummary.total_brp > 0
                          ? ((displaySummary.total_pie / displaySummary.total_brp) * 100).toFixed(1)
                          : "0"}%
                        del total
                      </p>
                    </div>
                    <Badge variant="pie">PIE</Badge>
                  </CardContent>
                </Card>
              </StaggerItem>
              <StaggerItem>
                <Card className="border-l-4 border-l-amber-500">
                  <CardContent className="p-4 flex items-center justify-between">
                    <div>
                      <p className="text-xs text-muted-foreground">BRP Normal</p>
                      <p className="text-xl font-bold text-amber-600 dark:text-amber-400">
                        <AnimatedNumber value={displaySummary.total_normal} format={(n) => formatCLP(n)} />
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {displaySummary.total_brp > 0
                          ? ((displaySummary.total_normal / displaySummary.total_brp) * 100).toFixed(1)
                          : "0"}%
                        del total
                      </p>
                    </div>
                    <Badge variant="normal">Normal</Badge>
                  </CardContent>
                </Card>
              </StaggerItem>
            </StaggerContainer>

            {/* BRP Concept breakdown */}
            <StaggerContainer className="grid grid-cols-1 md:grid-cols-3 gap-4" staggerDelay={0.08}>
              <StaggerItem>
                <Card>
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 mb-1">
                      <Award className="h-4 w-4 text-indigo-500" />
                      <p className="text-xs text-muted-foreground">Reconocimiento Profesional</p>
                    </div>
                    <p className="text-xl font-bold">
                      <AnimatedNumber value={displaySummary.total_reconocimiento} format={(n) => formatCLP(n)} />
                    </p>
                  </CardContent>
                </Card>
              </StaggerItem>
              <StaggerItem>
                <Card>
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 mb-1">
                      <TrendingUp className="h-4 w-4 text-emerald-500" />
                      <p className="text-xs text-muted-foreground">Asignacion de Tramo</p>
                    </div>
                    <p className="text-xl font-bold">
                      <AnimatedNumber value={displaySummary.total_tramo} format={(n) => formatCLP(n)} />
                    </p>
                  </CardContent>
                </Card>
              </StaggerItem>
              <StaggerItem>
                <Card>
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 mb-1">
                      <Zap className="h-4 w-4 text-amber-500" />
                      <p className="text-xs text-muted-foreground">Asignacion Prioritarios</p>
                    </div>
                    <p className="text-xl font-bold">
                      <AnimatedNumber value={displaySummary.total_prioritarios} format={(n) => formatCLP(n)} />
                    </p>
                  </CardContent>
                </Card>
              </StaggerItem>
            </StaggerContainer>

            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <BRPDistributionChart
                sep={displaySummary.total_sep}
                pie={displaySummary.total_pie}
                normal={displaySummary.total_normal}
              />
              <DAEMCPEIPChart
                daem={displaySummary.total_daem}
                cpeip={displaySummary.total_cpeip}
              />
            </div>
          </>
        )}

        {/* Monthly trends chart */}
        {hasTrends && <MonthlyTrendsChart data={trends} />}

        {/* School distribution (session data only) */}
        {!selectedMonth && schoolSummary.length > 0 && (
          <SchoolDistributionChart data={schoolSummary} />
        )}

        {/* D3 Treemap (session data only) */}
        {!selectedMonth && records.length > 0 && schoolSummary.length > 0 && (
          <D3Treemap records={records} schoolSummary={schoolSummary} />
        )}

        {/* Quick links */}
        {!selectedMonth && (
          <StaggerContainer className="grid grid-cols-1 md:grid-cols-2 gap-4" staggerDelay={0.08}>
            <StaggerItem>
              <Link href="/results">
                <Card className="cursor-pointer">
                  <CardContent className="p-4 flex items-center gap-3">
                    <BarChart3 className="h-5 w-5 text-primary" />
                    <div>
                      <p className="font-medium text-sm">Ver Resultados Completos</p>
                      <p className="text-xs text-muted-foreground">{records.length} registros</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            </StaggerItem>
            <StaggerItem>
              <Link href="/multi-establecimiento">
                <Card className="cursor-pointer">
                  <CardContent className="p-4 flex items-center gap-3">
                    <Building2 className="h-5 w-5 text-emerald-500" />
                    <div>
                      <p className="font-medium text-sm">Multi-Establecimiento</p>
                      <p className="text-xs text-muted-foreground">Docentes en multiples escuelas</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            </StaggerItem>
          </StaggerContainer>
        )}
      </div>
    </PageTransition>
  );
}
